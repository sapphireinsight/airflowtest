from airflow import DAG
from datetime import datetime, timedelta
from hello_plugin.operators.simple_operator import simpleOperator

SNOWFLAKE_CONN_ID = 'simple-test-conn'
MYSQL_CONNECTION_ID = 'mysql_297_mti_476'
MYSQL_DATABASE = 'ankurint1Betacust'


default_args = {
  'owner': 'airflow',
}

dag = DAG(
    'sql_snowflake_test',
    default_args=default_args,
    # schedule_interval="@daily",
    # start_date=datetime(2021, 1, 1),
    # catchup=False,
    schedule_interval='0 0 1 * *', dagrun_timeout=timedelta(seconds=5), start_date=datetime(2023, 1, 1), catchup=False,
    tags=['example'],
)

sql_snowflake_op_with_params = simpleOperator(
    task_id='sql_snowflake_op_with_params',
    dag=dag,
    snowflake_conn_id=SNOWFLAKE_CONN_ID,
    sql_conn_id=MYSQL_CONNECTION_ID,
    sql_query="SELECT id, target_type, operator, target_activity_type_id, conditions from smart_list_rule;",
    sql_database=MYSQL_DATABASE,
    sql_table="smart_list_rule_temp_2",
    sql_table_columswithtype="id integer, target_type string, operator string, target_activity_type_id integer, conditions string",
)

sql_snowflake_op_with_params
