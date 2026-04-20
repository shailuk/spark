from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, rlike, count, col, when, try_divide, broadcast, trim
import logging
import sys
import argparse


# Initialize Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def job_process(file_path):
    try:
        # Initialize SparkSession
        spark = SparkSession.builder \
            .appName("HealthcareDataAnalysis") \
            .getOrCreate()

        logger.info("Spark session initialized.")

        # Resolve GCS path based on the file name
        logger.info(f"Input path resolved: {file_path}")

        #Schema Definition
        health_data_schema = StructType([
            StructField('patient_id', StringType(), True),
            StructField('age', StringType(), True),
            StructField('gender', StringType(), True),
            StructField('diagnosis_code', StringType(), True),
            StructField('diagnosis_description', StringType(), True),
            StructField('diagnosis_date', StringType(), True)
        ])

        # Read the data from GCS
        health_df = (spark.read
             .format("csv")
             .option("header", "true")
             .option("nullValue", "null") 
             .option("emptyValue", "")
             .option("dateFormat", "yyyy-MM-dd")   # Example: if your CSV has '2023-08-01'
             .schema(health_data_schema)
             .load(file_path) )

        logger.info(f"Data read from GCS (health_df): {health_df.count()} rows")

        lk_diagnosis_schema = StructType([
              StructField('diagnosis_code', StringType(), True),
              StructField('diagnosis_description', StringType(), True)
            ])

        lk_diagnosis_df = ( spark.read
                .format("csv")
                .option("header", "true")
                .option("nullValue", "null") 
                .option("emptyValue", "")
                .schema(lk_diagnosis_schema) 
                .load("gs://nsk-airflow-projects-gds-dev/healthcare-data-analysis/source_data/lk_diagnosis.csv") )

        logger.info(f"Data read from GCS (lk_diagnosis_df): {lk_diagnosis_df.count()} rows")

        #Declare variables for BigQuery
        bq_project = "project-0bc8a567-62b7-4045-a22"
        bq_dataset = "health_care"
        bq_table_name = "health_care_data_stg"

        ########### Data Validation #############
        #Check if all the mandatory columns are present - Patient_id, age and diagnosis_code 
        #Check if the diagnosis column is in correct format
        #Blank columns are read as nulls - This happens at spark.read level
        #Diagnosis to Diagnosis Description validation
        #Check if the age < 0. If yes, set as invalid

        regex_pattern = "^[A-Z][0-9]{3}$"

        ###### Data Transformation #############
        # 1) Disease Gender Ratio - Group by diagnosis code and calculate male to female ratio
        print("1) Disease Gender Ratio - Group by diagnosis code and calculate male to female ratio....")
        health_df.groupBy(col('diagnosis_code')) \
                .agg(count(when(col('gender') == 'M', 1)).alias('male_count'),
                    count(when(col('gender') == 'F', 1)).alias('female_count')) \
                .withColumn('disease_gender_ratio', try_divide(col('male_count') , col('female_count')) ) \
                .select(col('diagnosis_code'), col('male_count'), col('female_count'), col('disease_gender_ratio').cast('decimal(3,2)') ).show()


        #2) Most Common Diseases - Top 3 most common diseases
        print("2) Most Common Diseases - Top 3 most common diseases....")
        health_df.groupBy(col('diagnosis_code'), col('diagnosis_description') ) \
                .agg(count(col('patient_id')).alias('patient_count')) \
                .orderBy(col('patient_count').desc()) \
                .limit(3).show()

        #3) Breakdown by Age Category 
        print("3) Breakdown by Age Category....")
        health_df.withColumn('age_category', \
                                when(col('age') <= 30, 'Less than 30') \
                            .when( (col('age') >= 31) & (col('age') <= 40), 'Between 31 and 40') \
                            .when( (col('age') >= 41) & (col('age') <= 50), 'Between 41 and 50') \
                            .when( (col('age') >= 51) & (col('age') <= 59), 'Between 51 and 60') \
                            .otherwise('Senior') ) \
                            .groupBy(col('age_category'), col('diagnosis_code'), col('diagnosis_description')) \
                            .agg(count(col('patient_id')).alias('patient_count')) \
                            .orderBy(col('diagnosis_code'), col('diagnosis_description'), col('patient_count').desc()) \
                            .show()

        #4) Flag for senior citizens and check if the age < 0. If yes, set the age to 0
        print("4) Flag for senior citizens and check if the age < 0. If yes, set the age to 0....")
        health_df_final = health_df.alias('hd') \
                                   .withColumn('is_senior', when(col('age') >=60, 'yes').otherwise('no') ) \
                                   .join( broadcast(lk_diagnosis_df).alias('lk'), \
                                         (col('lk.diagnosis_code') == col('hd.diagnosis_code')) & \
                                         ( trim(col('lk.diagnosis_code')) == trim(col('hd.diagnosis_code')) ) \
                                         , how = 'left') \
                                   .withColumn(  'validation_status', \
                                                 when(col("hd.patient_id").isNull() | col('hd.age').isNull() | col('hd.diagnosis_code').isNull(), "Invalid Data") \
                                                .when(col("hd.diagnosis_code").rlike(regex_pattern) == False, "Invalid Data") \
                                                .when(col('hd.age') < 0, 'Invalid Data') \
                                                .when(col('lk.diagnosis_code').isNull(), 'Invalid Data') \
                                                .otherwise("Valid Data") ) \
                                    .select(col('hd.patient_id'), col('hd.age'), col('hd.gender'), col('hd.diagnosis_code'), \
                                                     col('hd.diagnosis_description'), col('hd.diagnosis_date') )

        health_df_final.show(5)

        invalid_data_count = health_df_final.filter(col('validation_status') == "Invalid Data").count()
        print(f"Number of Invalid records in the file : {invalid_data_count}")     

        #Load the data into BigQuery
        logger.info(f"Writing transformed data to BigQuery table: {bq_project}:{bq_dataset}.{bq_table_name}")
        health_df_final.write \
                .format("bigquery") \
                .option("table", f"{bq_project}:{bq_dataset}.{bq_table_name}") \
                .option("writeMethod", "direct") \
                .mode("overwrite") \
                .save()
        
        # 5) Disease trend over the week - Count of diagnosis by date - Looking at the entire historical data
        print('5) Disease trend over the week')
        bq_health_df = spark.read \
                .format("bigquery") \
                .option("table", f"{bq_project}:{bq_dataset}.{bq_table_name}") \
                .load()
        
        pivot_df = bq_health_df.groupBy(col('diagnosis_code'), col('diagnosis_description')) \
                    .pivot('diagnosis_date') \
                    .agg(count(col('patient_id')).alias('patient_count')) \
                    .fillna(0) # Replaces nulls with 0 for a cleaner report
                    
        pivot_df.show()

        logger.info("Data Transformation and Analysis completed successfully.")

    except Exception as e:
        logger.error(f"An error occurred: {e}")

    finally:
        spark.stop()
        logger.info("Spark session stopped.")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--file_date", help="The date of the file to process")
    args = parser.parse_args()
    file_date = args.file_date
    print(f"Processing data for date: {file_date}")

    # Now use file_date in your GCS path or BigQuery logic
    path = f"gs://nsk-airflow-projects-gds-dev/healthcare-data-analysis/source_data/health_data_{file_date}.csv" 
    # Call the main function with parsed arguments
    job_process(path)

if __name__ == "__main__":
    main() #main function reads the command line arguments and calls the job_process function with the file date argument

