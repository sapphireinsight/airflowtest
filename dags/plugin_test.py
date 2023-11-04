from hello_plugin.operators.hello_operator import HelloOperator
from datetime import datetime, timedelta
from airflow import DAG 



with DAG(dag_id='plugin_test', schedule_interval='*/5 * * * *', dagrun_timeout=timedelta(seconds=5), start_date=datetime(2023, 1, 1), catchup=False) as dag: 
    hello_task = HelloOperator(task_id="sample-task", name="foo_bar")

    hello_task