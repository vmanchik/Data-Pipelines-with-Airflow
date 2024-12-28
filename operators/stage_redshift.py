from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    """
    Custom Airflow Operator to stage data from S3 to Redshift.

    Attributes:
        ui_color (str): Color code for the operator in the Airflow UI.
        template_fields (tuple): Fields that will be templated by Jinja.
        copy_sql (str): SQL command template for copying data from S3 to Redshift.

    Methods:
        __init__: Initializes the operator with the specified parameters.
        execute: Executes the operator's logic to stage data from S3 to Redshift.
    """
    ui_color = '#358140'
    template_fields = ("s3_path",)
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION '{}'
        JSON '{}'
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 aws_credentials_id='',
                 table='',
                 s3_path='',
                 region='us-west-2',
                 json_path='auto',
                 *args, **kwargs):
        """
        Initializes the StageToRedshiftOperator.

        Args:
            redshift_conn_id (str): Airflow connection ID for Redshift.
            aws_credentials_id (str): Airflow connection ID for AWS credentials.
            table (str): The target Redshift table to copy data into.
            s3_path (str): The S3 path where the data is stored.
            region (str): The AWS region where the S3 bucket is located.
            json_path (str): The JSON path file to be used for data parsing.
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.
        """
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_path = s3_path
        self.region = region
        self.json_path = json_path

    def execute(self, context):
        """
        Executes the data staging from S3 to Redshift.

        This method fetches AWS credentials, clears the target Redshift table, 
        renders the S3 path, formats the SQL copy command, and runs it on Redshift.

        Args:
            context (dict): Airflow context dictionary.
        """
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from destination Redshift table")
        redshift.run(f"DELETE FROM {self.table}")

        self.log.info("Copying data from S3 to Redshift")
        rendered_s3_path = self.s3_path.format(**context)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            rendered_s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.region,
            self.json_path
        )
        redshift.run(formatted_sql)

        self.log.info(f"Successfully staged data to {self.table}")
