from airflow import DAG
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import json
import logging

# Configuración de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_TOPIC = "test_topic"

# Configuración del DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 3, 18),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

dag = DAG(
    "dag_kafka",
    default_args=default_args,
    schedule_interval=None,  # Se ejecuta manualmente
    catchup=False,
)

# 🔹 Función que genera los mensajes para Kafka
def produce_messages():
    """Genera 10 mensajes en formato JSON correctamente estructurados."""
    messages = [
        (f"mensaje_{i}", json.dumps({"mensaje": f"¡Mensaje {i} desde Airflow y Kafka!"}))  # Key y Value
        for i in range(10)
    ]
    logger.info(f"📤 Enviando {len(messages)} mensajes a Kafka...")
    return messages

# 🔹 Enviar mensajes a Kafka
produce_message = ProduceToTopicOperator(
    task_id="produce_kafka_messages",
    topic=KAFKA_TOPIC,
    kafka_config_id="kafka_default",
    producer_function=produce_messages,
    dag=dag,
)

def count_messages(**context):
    messages = context["task_instance"].xcom_pull(task_ids="produce_kafka_messages")
    count = len(messages) if messages else 0
    logger.info(f"📊 Se han generado {count} mensajes en Kafka.")

count_messages_task = PythonOperator(
    task_id="count_kafka_messages",
    python_callable=count_messages,
    provide_context=True,
    dag=dag,
)


produce_message >> count_messages_task
