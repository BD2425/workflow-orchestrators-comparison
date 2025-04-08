from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import random

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 1),
    'retries': 1
}

dag = DAG('dag_random', default_args=default_args, schedule_interval='@daily',
    catchup=False)

def generar_numero():
    numero = random.randint(1, 100)
    print(f"Número generado: {numero}")
    
tarea = BashOperator(
    task_id='imprimir_mensaje',
    bash_command='echo "¡Hola desde Airflow!"',
    dag=dag
)

tarea_python = PythonOperator(
    task_id='generar_numero',
    python_callable=generar_numero,
    dag=dag
)

tarea >> tarea_python