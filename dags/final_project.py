from datetime import datetime, timedelta
import pendulum
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from operators.stage_redshift import StageToRedshiftOperator
from operators.load_fact import LoadFactOperator
from operators.load_dimension import LoadDimensionOperator
from operators.data_quality import DataQualityOperator
from helpers import SqlQueries

default_args = {
    'owner': 'udacity',
    'start_date': pendulum.now(),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'catchup': False,
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='@hourly'
)
def final_project():
    """
    DAG for ETL process: Load and transform data from S3 to Redshift.

    This DAG orchestrates the following steps:
    1. Stage event and song data from S3 to Redshift.
    2. Load the songplays fact table.
    3. Load dimension tables (users, songs, artists, time).
    4. Perform data quality checks on loaded tables.

    Tasks:
    - Begin_execution: Start the DAG execution.
    - Stage_events: Copy event data from S3 to Redshift staging table.
    - Stage_songs: Copy song data from S3 to Redshift staging table.
    - Load_songplays_fact_table: Transform and load data into songplays fact table.
    - Load_user_dim_table: Load data into users dimension table.
    - Load_song_dim_table: Load data into songs dimension table.
    - Load_artist_dim_table: Load data into artists dimension table.
    - Load_time_dim_table: Load data into time dimension table.
    - Run_data_quality_checks: Perform quality checks on all loaded tables.
    - Stop_execution: Mark the end of DAG execution.

    Task Dependencies:
    Begin_execution >> [Stage_events, Stage_songs] >> Load_songplays_fact_table >>
    [Load_user_dim_table, Load_song_dim_table, Load_artist_dim_table, Load_time_dim_table] >>
    Run_data_quality_checks >> Stop_execution

    Returns:
    None
    """
    start_operator = DummyOperator(task_id='Begin_execution')

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        table='staging_events',
        s3_bucket='victor-manchik',
        s3_key='log-data',
        region='us-east-1',
        copy_json_option='s3://victor-manchik/log_json_path.json'
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        table='staging_songs',
        s3_bucket='victor-manchik',
        s3_key='song-data',
        region='us-east-1',
        copy_json_option='auto'
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id='redshift',
        table='songplays',
        sql=SqlQueries.songplay_table_insert
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id='redshift',
        table='users',
        sql=SqlQueries.user_table_insert
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id='redshift',
        table='songs',
        sql=SqlQueries.song_table_insert
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id='redshift',
        table='artists',
        sql=SqlQueries.artist_table_insert
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id='redshift',
        table='time',
        sql=SqlQueries.time_table_insert
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id='redshift',
        tables=['songplays', 'users', 'songs', 'artists', 'time']
    )

    end_operator = DummyOperator(task_id='Stop_execution')

    # Define task dependencies
    start_operator >> [stage_events_to_redshift, stage_songs_to_redshift]
    [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
    load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, 
                             load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks
    run_quality_checks >> end_operator

final_project_dag = final_project()
