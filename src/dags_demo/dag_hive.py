from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.hive.hooks.hive import HiveServer2Hook
from datetime import datetime, timedelta
import logging

# Configuración de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuración básica de la DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 3, 18),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

dag = DAG(
    "dag_hive",
    default_args=default_args,
    schedule_interval=None,  # Se ejecuta manualmente
    catchup=False,
)

# Función para crear la base de datos en Hive
def create_database():
    try:
        hive_hook = HiveServer2Hook(hiveserver2_conn_id="hive_default")
        conn = hive_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute("CREATE DATABASE IF NOT EXISTS tienda")
        cursor.close()
        conn.close()
        logger.info("✅ Base de datos 'tienda' creada correctamente en Hive")
    except Exception as e:
        logger.error(f"❌ Error al crear la base de datos en Hive: {e}")
        raise

# Función para crear la tabla en Hive
def create_table():
    try:
        hive_hook = HiveServer2Hook(hiveserver2_conn_id="hive_default")
        conn = hive_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS tienda.usuarios (
                id INT,
                nombre STRING,
                email STRING
            )
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE
        """)
        cursor.close()
        conn.close()
        logger.info("✅ Tabla creada correctamente en Hive")
    except Exception as e:
        logger.error(f"❌ Error al crear la tabla en Hive: {e}")
        raise

# Función para insertar datos hardcodeados en Hive
def insert_data():
    try:
        hive_hook = HiveServer2Hook(hiveserver2_conn_id="hive_default")
        conn = hive_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO tienda.usuarios VALUES
            (1, 'Alice', 'alice@example.com'),
            (2, 'Bob', 'bob@example.com'),
            (3, 'Charlie', 'charlie@example.com')
        """)
        cursor.close()
        conn.close()
        logger.info("✅ Datos insertados correctamente en Hive")
    except Exception as e:
        logger.error(f"❌ Error al insertar datos en Hive: {e}")
        raise

# Función para consultar datos desde Hive
def query_hive():
    try:
        hive_hook = HiveServer2Hook(hiveserver2_conn_id="hive_default")
        conn = hive_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM tienda.usuarios LIMIT 5")
        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        logger.info("Resultados de Hive:")
        for row in rows:
            logger.info(row)
    except Exception as e:
        logger.error(f"❌ Error al consultar Hive: {e}")
        raise

# Definición de tareas en Airflow

create_database_task = PythonOperator(
    task_id="create_hive_database",
    python_callable=create_database,
    dag=dag,
)

create_table_task = PythonOperator(
    task_id="create_hive_table",
    python_callable=create_table,
    dag=dag,
)

insert_data_task = PythonOperator(
    task_id="insert_hive_data",
    python_callable=insert_data,
    dag=dag,
)

query_data_task = PythonOperator(
    task_id="query_hive_data",
    python_callable=query_hive,
    dag=dag,
)

# 🔗 Definir el flujo de ejecución de las tareas
create_database_task >> create_table_task >> insert_data_task >> query_data_task