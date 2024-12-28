# Project: Data Pipelines with Apache Airflow

As part of this project, the source data in S3 bucket in JSON format is procesed and loaded into a data warehouse in Amazone Redshift Serverless. The pipelines consist of reusable tasks and are built using
the TaskFlow API paradigm introduced as part of Apache Airflow 2.0.  The pipeline development process involved setting up the Redshift intance, configuring reshift role, copying raw data into s3 buckets, building Apache Airflow custom operators to perform staging of the data in local dvelopment invironment using VS Code and Docker image, trigering the data pipline in the Airflow ui to insert data into Redshift and run data quality checks, finally querying data in AWS Redshift to confirm that data have been successfully loaded.

### Steps

  - Create an IAM user in AWS
  - Configure Redshift Serverless
  - Store AWS Credentials in Airflow UI
  - Store Reshift connection endpoint in Ariflow UI
  - Copy log_data and song_data to an S3 bucket
  - Use the project starter and with the DAG template, the operators, and helper code
  - Configure the DAG with the data quality checks
  - Ensure that the DAG runs successfully

### DAG Graph
![DAG Graph](images/DAG_graph.png "This is image of final project DAG graph")

### Airflow Successful Run
![Successful Run of DAG](images/Airflow_successful_run.png "This is image of successful run of final projct DAG process")

