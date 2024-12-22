# Project: Data-Pipelines-with-Airflow
This project involves building data pipelines using Apache Airflow for Sparkify, a music streaming company, as part of Udacity Data Engineering with AWS program. The pipelines consist of reusable tasks.

The pipelines perform ETL process by moving data stored in JSON format in S3 to Amazon Redshift. The process involved building Apache Airflow custom operators to perform staging of the data, inserting data into Redshift, and running data quality checks. The steps involved are:
  - Create an IAM user in AWS
  - Configure Redshift Serverless
  - Connect Apache Airflow to AWS using AWS Credentials
  - Connect Apache Airflow to AWS Redshift
  - Copy log_data and song_data to an S3 bucket
  - Use the project starter and with the DAG template, the operators, and helper class folders
  - Configure the DAG with the data quality checks
  - Ensure that the DAG runs successfully
