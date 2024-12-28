# Project: Data Pipelines with Apache Airflow

As part of this project, the source data in the S3 bucket in JSON format is processed and loaded into a data warehouse in Amazon Redshift Serverless. The pipelines consist of reusable tasks and are built using the TaskFlow API paradigm introduced as part of Apache Airflow 2.0. The pipeline development process involved setting up the Redshift instance, configuring Redshift roles, and copying raw data into the S3 bucket.

### Steps

- Create an IAM user in AWS
- Configure Redshift Serverless
- Store AWS Credentials in Airflow UI
- Store Redshift connection endpoint in Airflow UI
- Copy log_data and song_data to an S3 bucket
- Use the project starter with the DAG template, the operators, and helper code
- Configure the DAG with the data quality checks
- Ensure that the DAG runs successfully

### DAG Graph
![DAG Graph](images/DAG_graph.png "This is an image of the final project DAG graph")

### Airflow Successful Run
![Successful Run of DAG](images/Airflow_successful_run.png "This is an image of a successful run of the final project DAG process")

### Post-ETL SQL Query in Redshift Serverless
![Redshift Query](images/Redshift_Serverless_post_ETL_query.png "This is an image of a SQL query in Redshift")
