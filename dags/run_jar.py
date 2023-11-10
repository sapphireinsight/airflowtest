from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
import os
import sys


with DAG(
    dag_id="runjar",
    schedule_interval="@daily",
    start_date=datetime(2021, 1, 1),
    catchup=False,
):
  run_jar_task= BashOperator(
    task_id = 'runjar',
    bash_command = 'java -cp /opt/airflow/jars/javaApp-1.0-SNAPSHOT.jar JavaApp'
    # bash_command = 'ls'
  )
  run_jar_task
