from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

SNOWFLAKE_CONN_ID = 'snowflake-test-conn'
SNOWFLAKE_SCHEMA = 'INSIGHTS'
SNOWFLAKE_STAGE = 'stage_name'
SNOWFLAKE_WAREHOUSE = 'B2B_INSIGHTS_COMPUTE_WH'
SNOWFLAKE_DATABASE = 'B2B_111BBB999_DB'
SNOWFLAKE_ROLE = 'B2B_INSIGHTS_111BBB999_WRITE_ROLE'
SNOWFLAKE_SAMPLE_TABLE = 'activity_log_item_temp'

# SQL commands
SQL_INSERT_STATEMENT = f"insert into b2b_111bbb999_db.insights.activity_log_item_temp (activity_log) select OBJECT_CONSTRUCT('first_name', 'Mickey', 'last_name', 'Mouse')"

default_args = {
    'owner': 'airflow',
}

dag = DAG(
    'snowflake_test',
    default_args=default_args,
    schedule_interval='*/5 * * * *', dagrun_timeout=timedelta(seconds=5), start_date=datetime(2023, 1, 1), catchup=False,
    tags=['example'],
)

snowflake_op_with_params = SnowflakeOperator(
    task_id='snowflake_op_with_params',
    dag=dag,
    snowflake_conn_id=SNOWFLAKE_CONN_ID,
    sql=SQL_INSERT_STATEMENT,
    database=SNOWFLAKE_DATABASE,
    warehouse=SNOWFLAKE_WAREHOUSE,
    schema=SNOWFLAKE_SCHEMA,
    role=SNOWFLAKE_ROLE,
)

snowflake_op_with_params
