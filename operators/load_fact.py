from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.postgres.hooks.postgres import PostgresHook

class LoadFactOperator(BaseOperator):
    """
    Operator for loading data into fact tables in Redshift.

    This operator connects to a Redshift database and loads data into a specified
    fact table. It can optionally truncate the table before loading new data.

    Attributes:
        ui_color (str): The color of the operator in the Airflow UI.

    Args:
        redshift_conn_id (str): The connection ID for the Redshift database. Defaults to "redshift".
        table (str): The name of the fact table to load data into. Defaults to an empty string.
        sql (str): The SQL query to select data for loading into the fact table. Defaults to an empty string.
        truncate (bool): Whether to truncate the table before loading new data. Defaults to False.
        *args: Variable length argument list passed to the BaseOperator.
        **kwargs: Arbitrary keyword arguments passed to the BaseOperator.
    """
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 table="",
                 sql="",
                 truncate=False,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql
        self.truncate = truncate

    def execute(self, context):
        """
        Executes the loading of data into the fact table.

        This method connects to Redshift, optionally truncates the target table,
        and then loads data into the fact table using the provided SQL query.

        Args:
            context: The context passed from the Airflow runtime.

        Raises:
            Exception: If there's an error during the execution of SQL queries.
        """
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.truncate:
            self.log.info(f"Truncating fact table: {self.table}")
            redshift_hook.run(f"TRUNCATE TABLE {self.table}")

        self.log.info(f"Loading data into fact table {self.table}")
        redshift_hook.run(f"INSERT INTO {self.table} {self.sql}")

        self.log.info(f"Fact table {self.table} loaded successfully")
