from pendulum import datetime
from airflow import DAG
from hello_plugin.operators.simple_operator import simpleOperator


SNOWFLAKE_CONN_ID = 'simple-test-conn'

MYSQL_CONNECTION_ID = 'mysql_297_mti_476'
MYSQL_DATABASE = 'ankurint1Betacust'

with DAG(
    dag_id="simple_dag",
    schedule_interval="@daily",
    start_date=datetime(2021, 1, 1),
    # render Jinja template as native Python object
    render_template_as_native_obj=True,
    catchup=False,
):
  sync_smart_list_rule = simpleOperator(
      task_id='write_smart_list_rule',
      snowflake_conn_id=SNOWFLAKE_CONN_ID,
      sql_conn_id=MYSQL_CONNECTION_ID,
      sql_query="SELECT id, target_type, operator, target_activity_type_id, conditions from smart_list_rule;",
      sql_database=MYSQL_DATABASE,
      sql_table="smart_list_rule_temp_2",
      sql_table_columswithtype="id integer, target_type string, operator string, target_activity_type_id integer, conditions string",
  )
  
  sync_activity_type = simpleOperator(
      task_id='write_activity_type',
      snowflake_conn_id=SNOWFLAKE_CONN_ID,
      sql_conn_id=MYSQL_CONNECTION_ID,
      sql_query="SELECT id, name from activity_type;",
      sql_database=MYSQL_DATABASE,
      sql_table="activity_type_temp_2",
      sql_table_columswithtype="id integer, name string",
  )

  [sync_smart_list_rule,sync_activity_type]
