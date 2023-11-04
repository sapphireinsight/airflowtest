from datetime import datetime, timedelta
from airflow import DAG 
from airflow.operators.python_operator import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook

def show_table(mysql_conn_id, sql, database, **kwargs):
    """Print results of sql query """
    hook = MySqlHook(mysql_conn_id=mysql_conn_id, schema=database)
    df = hook.get_pandas_df(sql=sql)
    print(df.to_string())
    
with DAG(dag_id='mysql_test', schedule_interval='*/5 * * * *', dagrun_timeout=timedelta(seconds=5), start_date=datetime(2023, 1, 1), catchup=False) as dag: 

    show_table = PythonOperator(
        task_id="show_table",
        python_callable=show_table,
        op_kwargs={"mysql_conn_id": "mysql_297_mti_476", "sql": """select * from activity_type;""", "database": "ankurint1Betacust"}, 
        provide_context=True,
    )
    
    show_table 
