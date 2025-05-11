import luigi
import json
import logging
import smtplib
import os
import subprocess
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from hdfs import InsecureClient
from kafka import KafkaProducer
from dotenv import load_dotenv
from pyhive import hive

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

load_dotenv()
sender = os.environ['LUIGI_EMAIL']
password = os.environ['LUIGI_EMAIL_PASSWORD']
recipient = os.environ['LUIGI_EMAIL_TO']

# Configuración de rutas
LOCAL_CSV = '/opt/luigi/data/yellow_tripdata_2015-01.csv'
HDFS_PATH = '/user/hive/luigi/nyc_taxi/trips_2015/yellow_tripdata_2015-01.csv'
KAFKA_TOPIC = "etl_topic"
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

# Subir el CSV a HDFS
class UploadToHDFS(luigi.Task):
    def run(self):
        client = InsecureClient('http://namenode:9870', user='hdfs') 

        logging.info(f"Uploading file {LOCAL_CSV} to HDFS: {HDFS_PATH}")
        
        with open(LOCAL_CSV, 'rb') as local_file:
            client.write(HDFS_PATH, local_file, overwrite=True)
        
        logging.info("Upload to HDFS completed.")
        
        with self.output().open('w') as f:
            f.write("HDFS upload completed.")

    def output(self):
        return luigi.LocalTarget('hdfs_upload_complete.txt')

# Crear la tabla en Hive
class CreateHiveTable(luigi.Task):
    def requires(self):
        return UploadToHDFS()

    def run(self):
        conn = hive.Connection(host='hiveserver2', port=10000, username='hive')
        cursor = conn.cursor()

        try:
            drop_table_query = "DROP TABLE IF EXISTS nyc_taxi_raw"
            cursor.execute(drop_table_query)
            logging.info("Dropped existing Hive table nyc_taxi_raw if it existed")

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
                LOCATION '/user/hive/luigi/nyc_taxi/trips_2015'
            """
            cursor.execute(create_table_query)
            logging.info("Table in Hive created successfully")

            with self.output().open('w') as f:
                f.write("Hive table created successfully.")

        except Exception as e:
            logging.error(f"Error creating Hive table: {e}")

    def output(self):
        return luigi.LocalTarget('hive_table_created.txt')

# Limpiar los datos en Hive
class CleanDataInHive(luigi.Task):
    def requires(self):
        return CreateHiveTable()

    def run(self):
        conn = hive.Connection(host='hiveserver2', port=10000, username='hive')
        cursor = conn.cursor()

        try:
            drop_table_query = "DROP TABLE IF EXISTS nyc_taxi_clean"
            cursor.execute(drop_table_query)
            logging.info("Dropped existing Hive table nyc_taxi_clean if it existed")

            clean_query = """
                CREATE TABLE IF NOT EXISTS nyc_taxi_clean AS
                SELECT * FROM nyc_taxi_raw
                WHERE passenger_count > 0
            """
            cursor.execute(clean_query)
            logging.info("Cleaned data saved in Hive")

            with self.output().open('w') as f:
                f.write("Data cleaned in Hive successfully.")

        except Exception as e:
            logging.error(f"Error cleaning data in Hive: {e}")

    def output(self):
        return luigi.LocalTarget('hive_data_cleaned.txt')

# Ejecutar consultas en Hive
class RunHiveQueries(luigi.Task):
    def requires(self):
        return CleanDataInHive()

    def run(self):
        conn = hive.Connection(host='hiveserver2', port=10000, username='hive')
        cursor = conn.cursor()

        try:
            query_1 = """
                SELECT payment_type, COUNT(*) AS num_trips
                FROM nyc_taxi_clean
                GROUP BY payment_type
                ORDER BY num_trips DESC
            """
            cursor.execute(query_1)
            result_1 = cursor.fetchall()
            logging.info(f"Query 1 (trips by payment type): {result_1}")

            query_2 = """
                SELECT TO_DATE(tpep_pickup_datetime) AS trip_date, COUNT(*) AS num_trips
                FROM nyc_taxi_clean
                GROUP BY TO_DATE(tpep_pickup_datetime)
                ORDER BY trip_date
            """
            cursor.execute(query_2)
            result_2 = cursor.fetchall()
            logging.info(f"Query 2 (trips by day): {result_2}")

            # Guardar resultados en un archivo local
            result_data = {"result_1": result_1, "result_2": result_2}
            with self.output().open('w') as f:
                json.dump(result_data, f)

        except Exception as e:
            logging.error(f"Error running Hive queries: {e}")
            raise

    def output(self):
        return luigi.LocalTarget('/tmp/hive_query_results.json')

# Publicar evento en Kafka
class PublishKafkaEvent(luigi.Task):
    def requires(self):
        return RunHiveQueries()

    def run(self):
        with self.input().open('r') as f:
            result = json.load(f)

        payload = json.dumps({
            "status": "done",
            "file": HDFS_PATH,
            "timestamp": datetime.now().isoformat(),
            "results": result
        }).encode('utf-8')

        logging.info(f"Publishing Kafka event: {payload}")
        producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
        producer.send(KAFKA_TOPIC, payload)
        producer.flush()
        logging.info("Kafka event published.")

        with self.output().open('w') as f:
            f.write("Kafka event published.")

    def output(self):
        return luigi.LocalTarget('kafka_event_published.txt')

# Enviar notificación por correo
class SendEmailNotification(luigi.Task):
    def requires(self):
        return [RunHiveQueries(), PublishKafkaEvent()]

    def output(self):
        return luigi.LocalTarget('email_sent.txt')

    def run(self):
        with self.input()[0].open('r') as f:
            data = json.load(f)
        result_1 = data["result_1"]
        result_2 = data["result_2"]

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

        self.send_email(
            subject='[Luigi] ETL completed',
            html_content=content
        )

        with self.output().open('w') as f:
            f.write("Email sent.")

    def send_email(self, subject, html_content):
        msg = MIMEMultipart('alternative')
        msg['Subject'] = subject
        msg['From'] = sender
        msg['To'] = recipient

        msg.attach(MIMEText(html_content, 'html'))

        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
            server.login(sender, password)
            server.sendmail(sender, recipient, msg.as_string())
