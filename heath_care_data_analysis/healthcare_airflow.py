
from airflow import DAG
from airflow.models.param import Param
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from datetime import datetime, timedelta
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


# Define default arguments for the DAG
default_args = {  
    'owner' : 'airflow',
    'start_date' : '2024-06-01',
    'retries' : 0
    #,'retry_delay' : timedelta(minutes=2)  # Using timedelta is more readable
}

#Define the DAG
with DAG(
    dag_id = 'healthcare_airflow',
    default_args = default_args,
    schedule_interval = None,  #Manual Run
    #schedule_interval='@daily',
    catchup=False,  #Set this to true for back-filling data for the past dates. It will run the DAG for all the dates between start_date and current date
    params = {
        "file_date" : Param("20240601", type="string", description="Date of the file to load in YYYYMMDD format") }
) as dag:

    #submit the spark job to Dataproc Serverless
    submit_pyspark_batch = DataprocCreateBatchOperator(
        task_id='submit_pyspark_job', 
        project_id = 'project-0bc8a567-62b7-4045-a22',
        region = 'us-central1',
        batch = {
            "pyspark_batch": {
            "main_python_file_uri": "gs://nsk-airflow-projects-gds-dev/healthcare-data-analysis/spark_job/healthcare_data_analysis.py", 
            "args": ["--file_date", "{{ params.file_date }}"]
            } } )
    
    #Define BigQuery upsert task
    upsert_to_bq = BigQueryInsertJobOperator(
    task_id="upsert_health_data",
    location="us-central1",
    configuration={
        "query": {
            "query": "{% include 'sql/upsert_health_data.sql' %}", # Path to your SQL file
            "useLegacySql": False,
        }
    },
    params={
        "project_id": "project-0bc8a567-62b7-4045-a22",
        "dataset": "health_care"
    } )

    #Move file from one bucket to another
    move_file = GCSToGCSOperator(
        task_id = 'move_file',
        source_bucket = 'nsk-airflow-projects-gds-dev',
        source_object = 'healthcare-data-analysis/source_data/*.csv',
        destination_bucket = 'nsk-airflow-projects-gds-dev',   
        destination_object = 'healthcare-data-analysis/archive/', 
        move_object=True, # Deletes from raw_data_bucket
        replace=False,    # Prevents accidental overwrites
        gcp_conn_id = 'google_cloud_default' )
    

#Dependency Flow
    submit_pyspark_batch >> upsert_to_bq >> move_file