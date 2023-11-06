from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from hello_plugin.operators.sqlToSnowflakeOperator import SqlToSnowflakeOperator

SNOWFLAKE_CONN_ID = 'snowflake-test-conn'
SNOWFLAKE_SCHEMA = 'INSIGHTS'
SNOWFLAKE_STAGE = 'stage_name'
SNOWFLAKE_WAREHOUSE = 'B2B_INSIGHTS_COMPUTE_WH'
SNOWFLAKE_DATABASE = 'B2B_111BBB999_DB'
SNOWFLAKE_ROLE = 'B2B_INSIGHTS_111BBB999_WRITE_ROLE'


MYSQL_CONNECTION_ID = 'mysql_297_mti_476'
MYSQL_QUERY = 'select id, name from activity_type;'


default_args = {
  'owner': 'airflow',
}

dag = DAG(
    'sql_snowflake_test',
    default_args=default_args,
    schedule_interval='*/5 * * * *', dagrun_timeout=timedelta(seconds=5), start_date=datetime(2023, 1, 1), catchup=False,
    tags=['example'],
)

sql_snowflake_op_with_params = SqlToSnowflakeOperator(
    task_id='sql_snowflake_op_with_params',
    dag=dag,
    snowflake_conn_id=SNOWFLAKE_CONN_ID,
    database=SNOWFLAKE_DATABASE,
    warehouse=SNOWFLAKE_WAREHOUSE,
    schema=SNOWFLAKE_SCHEMA,
    role=SNOWFLAKE_ROLE,
    sql_conn_id=MYSQL_CONNECTION_ID,
    query=MYSQL_QUERY,
    parameters={"database": "ankurint1Betacust"},
)

sql_snowflake_op_with_params
