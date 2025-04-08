from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 1),
    'retries': 1
}

dag = DAG('dag_hello', default_args=default_args, schedule_interval='@daily',
    catchup=False)

tarea = BashOperator(
    task_id='imprimir_mensaje',
    bash_command='echo "¡Hola desde Airflow!"',
    dag=dag
)