from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 1),
    'retries': 1
}

dag = DAG('dag_hdfs', default_args=default_args, schedule_interval='@daily', catchup=False)

def leer_hdfs():
    hdfs_hook = WebHDFSHook(webhdfs_conn_id='hdfs_default')
    contenido = hdfs_hook.read_file('/user/airflow/datos.txt')
    print(f"Contenido en HDFS:\n{contenido}")

leer_tarea = PythonOperator(
    task_id='leer_hdfs',
    python_callable=leer_hdfs,
    dag=dag
)
