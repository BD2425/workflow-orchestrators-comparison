from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.email_operator import EmailOperator
from airflow.providers.apache.hive.hooks.hive import HiveServer2Hook
from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from datetime import datetime

import json
import logging

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'email': ['bdworkfloworchestrator@gmail.com'],
    'email_on_failure': True
}

dag = DAG(
    'etl_nyc_taxi_hdfs_hive',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description='ETL DAG: csv to HDFS, create Hive table, clean data, publish to Kafka and notify via email',
)

# Configuración de rutas
LOCAL_CSV = '/opt/airflow/data/yellow_tripdata_2015-01.csv'
HDFS_PATH = '/user/hive/warehouse/nyc_taxi/trips_2015/yellow_tripdata_2015-01.csv'
KAFKA_TOPIC = "etl_topic"

# Paso 1: Subir CSV a HDFS
def upload_to_hdfs():
    hdfs_hook = WebHDFSHook(webhdfs_conn_id="hdfs_default") 
    hdfs_hook.load_file(LOCAL_CSV, HDFS_PATH, overwrite=True)
    logging.info(f"File {LOCAL_CSV} uploaded successfully to {HDFS_PATH}.")

upload_to_hdfs_task = PythonOperator(
    task_id='upload_to_hdfs',
    python_callable=upload_to_hdfs,
    dag=dag
)

# Paso 2: Crear tabla externa en Hive
def create_hive_table():
    hive_hook = HiveServer2Hook(hiveserver2_conn_id='hive_default')
    create_table_query = f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS nyc_taxi_raw (
            VendorID INT,
            tpep_pickup_datetime STRING,
            tpep_dropoff_datetime STRING,
            passenger_count INT,
            trip_distance FLOAT,
            pickup_longitude FLOAT,
            pickup_latitude FLOAT,
            RateCodeID INT,
            store_and_fwd_flag STRING,
            dropoff_longitude FLOAT,
            dropoff_latitude FLOAT,
            payment_type INT,
            fare_amount FLOAT,
            extra FLOAT,
            mta_tax FLOAT,
            tip_amount FLOAT,
            tolls_amount FLOAT,
            improvement_surcharge FLOAT,
            total_amount FLOAT
        )
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
        STORED AS TEXTFILE
        LOCATION '/user/hive/warehouse/nyc_taxi/trips_2015'
    """
    hive_hook.run(create_table_query)
    logging.info("Table in Hive created successfully")

create_hive_table_task = PythonOperator(
    task_id='create_hive_table',
    python_callable=create_hive_table,
    dag=dag
)

# Paso 3: Limpiar los datos
def clean_data_in_hive():
    hive_hook = HiveServer2Hook(hiveserver2_conn_id='hive_default')
    clean_query = """
        CREATE TABLE IF NOT EXISTS nyc_taxi_clean AS
        SELECT * FROM nyc_taxi_raw
        WHERE passenger_count > 0
    """
    hive_hook.run(clean_query)
    logging.info("Datos limpios guardados en Hive")

clean_data_hive = PythonOperator(
    task_id='clean_data_hive',
    python_callable=clean_data_in_hive,
    dag=dag
)

# Paso 4: Publicar evento en Kafka
def generate_etl_message():
    payload = json.dumps({
        "status": "done",
        "file": HDFS_PATH,
        "timestamp": datetime.now().isoformat()
    })
    return [("etl_event", payload)]

publish_kafka_event = ProduceToTopicOperator(
    task_id="publish_kafka_event",
    topic=KAFKA_TOPIC,
    kafka_config_id="kafka_default",
    producer_function=generate_etl_message,
    dag=dag
)

# Paso 5: Notificación por correo
email_notify = EmailOperator(
    task_id='email_notify',
    to=Variable.get("email_recipient"),
    subject='[Airflow] ETL completada',
    html_content=f'<p>El ETL ha terminado correctamente. Archivo cargado en <code>{HDFS_PATH}</code>.</p>',
    dag=dag
)

# Dependencias del DAG
upload_to_hdfs_task >> create_hive_table_task >> clean_data_hive >> publish_kafka_event >> email_notify

