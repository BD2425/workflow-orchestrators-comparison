import luigi
import json
import logging
from datetime import datetime
from luigi.contrib.hadoop import HdfsClient
from luigi.contrib.hive import HiveClient
from luigi.contrib.kafka import KafkaProducer
from dotenv import load_dotenv
import os

load_dotenv()
sender = os.environ['AIRFLOW__SMTP__SMTP_USER']
password = os.environ['AIRFLOW__SMTP__SMTP_PASSWORD']
recipient = os.environ['AIRFLOW__SMTP__SMTP_USER']

# Configuración de rutas
LOCAL_CSV = '/opt/airflow/data/yellow_tripdata_2015-01.csv'
HDFS_PATH = '/user/hive/warehouse/nyc_taxi/trips_2015/yellow_tripdata_2015-01.csv'
KAFKA_TOPIC = "etl_topic"

# Subir el CSV a HDFS
class UploadToHDFS(luigi.Task):
    def run(self):
        # Usamos HdfsClient de Luigi para interactuar con HDFS
        hdfs_client = HdfsClient()
        hdfs_client.upload(HDFS_PATH, LOCAL_CSV)
        logging.info(f"File {LOCAL_CSV} uploaded successfully to {HDFS_PATH}")

# Crear la tabla en Hive
class CreateHiveTable(luigi.Task):
    def requires(self):
        return UploadToHDFS()

    def run(self):
        hive_client = HiveClient()
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
        hive_client.run(create_table_query)
        logging.info("Table in Hive created successfully")

# Limpiar los datos en Hive
class CleanDataInHive(luigi.Task):
    def requires(self):
        return CreateHiveTable()

    def run(self):
        hive_client = HiveClient()
        clean_query = """
            CREATE TABLE IF NOT EXISTS nyc_taxi_clean AS
            SELECT * FROM nyc_taxi_raw
            WHERE passenger_count > 0
        """
        hive_client.run(clean_query)
        logging.info("Cleaned data saved in Hive")

# Ejecutar consultas en Hive
class RunHiveQueries(luigi.Task):
    def requires(self):
        return CleanDataInHive()

    def run(self):
        hive_client = HiveClient()

        query_1 = """
            SELECT payment_type, COUNT(*) AS num_trips
            FROM nyc_taxi_clean
            GROUP BY payment_type
            ORDER BY num_trips DESC;
        """
        result_1 = hive_client.get(query_1)
        logging.info(f"Query 1 (trips by payment type): {result_1}")

        query_2 = """
            SELECT TO_DATE(tpep_pickup_datetime) AS trip_date, COUNT(*) AS num_trips
            FROM nyc_taxi_clean
            GROUP BY TO_DATE(tpep_pickup_datetime)
            ORDER BY trip_date;
        """
        result_2 = hive_client.get(query_2)
        logging.info(f"Query 2 (trips by day): {result_2}")

        # Guardar resultados en un archivo local
        result_data = {"result_1": result_1, "result_2": result_2}
        with self.output().open('w') as f:
            json.dump(result_data, f)

    def output(self):
        return luigi.LocalTarget('/tmp/hive_query_results.json')

# Publicar evento en Kafka
class PublishKafkaEvent(luigi.Task):
    def requires(self):
        return RunHiveQueries()

    def run(self):
        result = self.input().open('r').read()
        payload = json.dumps({
            "status": "done",
            "file": HDFS_PATH,
            "timestamp": datetime.now().isoformat(),
            "results": result
        })
        logging.info(f"Publishing Kafka event with payload: {payload}")
        kafka_producer = KafkaProducer()
        kafka_producer.produce(KAFKA_TOPIC, payload)

# Enviar notificación por correo
class SendEmailNotification(luigi.Task):
    def requires(self):
        return RunHiveQueries()

    def output(self):
        return luigi.LocalTarget('email_sent.txt')

    def run(self):
        # Leer los resultados
        with self.input().open('r') as f:
            data = json.load(f)
        result_1 = data["query_1"]
        result_2 = data["query_2"]

        # Generar contenido del email
        content = f"""
        <p>The ETL process has completed successfully. Below are the results from the Hive queries:</p>
        <h3>Query 1: Number of trips by payment type</h3>
        <ul>
            {''.join([f"<li>Payment Type: {item[0]}, Number of Trips: {item[1]}</li>" for item in result_1])}
        </ul>
        <h3>Query 2: Number of trips by day</h3>
        <ul>
            {''.join([f"<li>Date: {item[0]}, Number of Trips: {item[1]}</li>" for item in result_2])}
        </ul>
        """

        # Enviar el correo
        self.send_email(
            subject='[Luigi] ETL completed',
            html_content=content
        )

        # Marcar tarea como completada
        with self.output().open('w') as f:
            f.write("Email sent.")

    def send_email(self, subject, html_content):
        msg = MIMEMultipart('alternative')
        msg['Subject'] = AIRFLOW__SMTP__SMTP_USER
        msg['From'] = sender
        msg['To'] = recipient

        msg.attach(MIMEText(html_content, 'html'))

        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
            server.login(sender, password)
            server.sendmail(sender, recipient, msg.as_string())
