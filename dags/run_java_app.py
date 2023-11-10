from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
import os
import sys


with DAG(
    dag_id="run_java_app",
    schedule_interval="@daily",
    start_date=datetime(2021, 1, 1),
    catchup=False,
):
  run_jar_task= BashOperator(
    task_id='run_java_app',
    bash_command='java -jar /opt/airflow/jars/javaApp-1.0-SNAPSHOT-all.jar'
  )
  run_jar_task
