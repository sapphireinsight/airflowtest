from airflow import DAG
from datetime import datetime, timedelta
from hello_plugin.operators.simple_operator import simpleOperator

from airflow.operators.python_operator import PythonOperator
from jnius import autoclass

SNOWFLAKE_CONN_ID = 'simple-test-conn'


MYSQL_CONNECTION_ID = 'mysql_297_mti_476'
MYSQL_QUERY = 'select id, name from activity_type;'
MYSQL_DATABASE = 'ankurint1Betacust'


default_args = {
  'owner': 'airflow',
}

# dag = DAG(
#     'sql_snowflake_test',
#     default_args=default_args,
#     # schedule_interval="@daily",
#     # start_date=datetime(2021, 1, 1),
#     # catchup=False,
#     schedule_interval='*/5 * * * *', dagrun_timeout=timedelta(seconds=5), start_date=datetime(2023, 1, 1), catchup=False,
#     tags=['example'],
# )
#
# sql_snowflake_op_with_params = simpleOperator(
#     task_id='sql_snowflake_op_with_params',
#     dag=dag,
#     snowflake_conn_id=SNOWFLAKE_CONN_ID,
#     sql_conn_id=MYSQL_CONNECTION_ID,
#     sql_query=MYSQL_QUERY,
#     sql_database=MYSQL_DATABASE,
# )
#
# sql_snowflake_op_with_params


def run_some_java_codes():
  Stack = autoclass('java.util.Stack')
  stack = Stack()
  stack.push('hello')
  stack.push('world')
  print(stack.pop()) # --> 'world'
  print(stack.pop()) # --> 'hello'


with DAG(
    'sql_snowflake_test',
    default_args=default_args,
    # schedule_interval="@daily",
    # start_date=datetime(2021, 1, 1),
    # catchup=False,
    schedule_interval='*/5 * * * *', dagrun_timeout=timedelta(seconds=5), start_date=datetime(2023, 1, 1), catchup=False,
    tags=['example'],
):

  test_run_java_code = PythonOperator(
      task_id="test_run_java_code",
      python_callable=run_some_java_codes,
      op_kwargs={},
      provide_context=True,
  )

  sync_activity_type = simpleOperator(
      task_id='write_activity_type',
      snowflake_conn_id=SNOWFLAKE_CONN_ID,
      sql_conn_id=MYSQL_CONNECTION_ID,
      sql_query=MYSQL_QUERY,
      sql_database=MYSQL_DATABASE,
      sql_table="activity_type_temp_2",
      sql_table_columswithtype="id integer, name string"
  )

  sync_smart_list_rule = simpleOperator(
      task_id='write_smart_list_rule',
      snowflake_conn_id=SNOWFLAKE_CONN_ID,
      sql_conn_id=MYSQL_CONNECTION_ID,
      sql_query=MYSQL_QUERY,
      sql_database=MYSQL_DATABASE,
      sql_table="smart_list_rule",
      sql_table_columswithtype="id integer, target_type string, operator string, target_activity_type_id integer, conditions string"
  )

  test_run_java_code >> [sync_activity_type, sync_smart_list_rule]
