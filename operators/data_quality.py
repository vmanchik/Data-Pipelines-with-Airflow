from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.postgres.hooks.postgres import PostgresHook

class DataQualityOperator(BaseOperator):
    """
    Operator for performing data quality checks on Redshift tables.

    This operator connects to a Redshift database and performs basic data quality
    checks on specified tables. It verifies that each table contains records and
    raises an error if any table is empty.

    Attributes:
        ui_color (str): The color of the operator in the Airflow UI.
        redshift_conn_id (str): The connection ID for the Redshift database.
        tables (list): A list of table names to perform quality checks on.

    Args:
        redshift_conn_id (str): The connection ID for the Redshift database. Defaults to "redshift".
        tables (list): A list of table names to perform quality checks on. Defaults to an empty list.
        *args: Variable length argument list passed to the BaseOperator.
        **kwargs: Arbitrary keyword arguments passed to the BaseOperator.

    Raises:
        ValueError: If a table returns no results or contains zero rows.
    """
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 tables=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)

        for table in self.tables:
            self.log.info(f"Starting data quality check on table: {table}")
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")

            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {table} returned no results")

            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f"Data quality check failed. {table} contained 0 rows")

            self.log.info(f"Data quality on table {table} check passed with {records[0][0]} records")
