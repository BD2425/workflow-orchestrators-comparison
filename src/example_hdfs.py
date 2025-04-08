# Definir argumentos por defecto del DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 1),
    'retries': 1
}
# Inicializar el DAG
dag = DAG(
    'dag_hdfs_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
)
# URL de donde se descargará el archivo (simulación de extracción de datos)
URL = "https://people.sc.fsu.edu/~jburkardt/data/csv/hw_200.csv"
LOCAL_FILE = "/tmp/datos.csv"
HDFS_PATH = "/user/airflow/datos.csv"

# Función para descargar datos desde una URL
def descargar_datos():
    response = requests.get(URL)
    with open(LOCAL_FILE, "wb") as f:
        f.write(response.content)
    print("Archivo descargado exitosamente.")
    
# Función para subir el archivo a HDFS
def subir_a_hdfs():
hdfs_hook = HDFSHook(hdfs_conn_id='hdfs_default')
with open(LOCAL_FILE, 'rb') as file_data:
hdfs_hook.upload(HDFS_PATH, file_data)
print(f"Archivo subido a HDFS en {HDFS_PATH}")
# Función para leer el archivo desde HDFS y contar líneas
def contar_lineas_hdfs():
hdfs_hook = HDFSHook(hdfs_conn_id='hdfs_default')
file_content = hdfs_hook.read_file(HDFS_PATH)
num_lineas = len(file_content.strip().split("\n"))
print(f"El archivo en HDFS tiene {num_lineas} líneas.")
# Definir las tareas en Airflow
tarea_descargar = PythonOperator(
task_id='descargar_datos',
python_callable=descargar_datos,
dag=dag
)
tarea_subir_hdfs = PythonOperator(
task_id='subir_a_hdfs',
python_callable=subir_a_hdfs,
dag=dag
)
tarea_contar_lineas = PythonOperator(
task_id='contar_lineas_hdfs',
python_callable=contar_lineas_hdfs,
dag=dag
)
# Definir el flujo de ejecución
tarea_descargar >> tarea_subir_hdfs >> tarea_contar_lineas
