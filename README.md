# Project: Data-Pipelines-with-Airflow
This project involves building data pipelines using Apache Airflow for Sparkify, a music striming company, as part of Udacity Data Engineering with AWS program. The pipelines consist of reusable tasks and are dynamic, can be monitored, and allow for backfills.  In addition, the pipelines have data quality checks imbeded. 

The pipelines perform ETL process by moving data stored in JSON format in S3 to Amazon Redshift.  The process involved building Apache Airflow custom operators to perform staging of the data, inserting data into the data warehouse, and running data quality checks.  The project follows the following steps:
  - Create an IAM user in AWS
  - Configure Redsheft Serverless
  - Connect Apache Airflow to AWS using AWS Credentials
  - Connect Apache Airflow to AWS Redshift
  - Copy log_data and song_data to an S3 bucket
  - Use the project starter and with the dag tamplate, the operators, and helper class folders
  - Configure the DAG with the data quality
  - Ensure that the DAG runs successfully

