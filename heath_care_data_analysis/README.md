# Health Care Data Analysis

### **🚀 Overview**  <br/>

#### Build a Spark application that will process a CSV file(s) from a GCS bucket, perform certain transformations and validations on it, and then store the transformed data in a Bigquery table table.

### Architecture <br/>
**Ingestion**: Raw healthcare CSV data is uploaded to GCS. <br/>
**Processing**: A PySpark application running on Google Dataproc Serverless performs validations and transformations. <br/>
**Staging**: Transformed data is written to a BigQuery staging table using the Spark-BigQuery connector. <br/>
**Final Load**: An Airflow-managed SQL MERGE operation performs an idempotent upsert into the production table. <br/>

### Tech Stack <br/>
**Compute**: Apache Spark 3.x (PySpark) running on DataProc Serverless <br/>
**Orchestration**: Apache Airflow (Cloud Composer) <br/>
**Storage**: Google Cloud Storage (GCS) <br/>
**Warehouse**: Google BigQuery <br/>
**IDE**: VS Code
**Programming Language** : Python
