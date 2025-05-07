from dagster import job, op, get_dagster_logger
from hdfs import InsecureClient
from pyhive import hive
from confluent_kafka import Producer
from email.message import EmailMessage
import smtplib
import os
import json
from datetime import datetime

LOCAL_CSV = '/opt/dagster/app/data/yellow_tripdata_2015-01.csv'
HDFS_PATH = '/user/hive/dagster/nyc_taxi/trips_2015/yellow_tripdata_2015-01.csv'
KAFKA_TOPIC = "etl_topic"

@op
def upload_to_hdfs():
    logger = get_dagster_logger()
    client = InsecureClient('http://namenode:9870', user='hdfs')
    logger.info(f"Uploading file {LOCAL_CSV} to HDFS: {HDFS_PATH}")
    with open(LOCAL_CSV, 'rb') as local_file:
        client.write(HDFS_PATH, local_file, overwrite=True)
    logger.info("Upload to HDFS completed.")
    return "uploaded"

@op
def create_hive_table(previous_step: str):
    logger = get_dagster_logger()
    conn = hive.Connection(host='hiveserver2', port=10000, database='default')
    cursor = conn.cursor()
    query = f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS nyc_taxi_raw_dagster (
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
        LOCATION '/user/hive/dagster/nyc_taxi/trips_2015'
    """
    cursor.execute(query)
    logger.info("Hive table nyc_taxi_raw_dagster created.")
    return "table_created"

@op
def clean_data_in_hive(previous_step: str):
    logger = get_dagster_logger()
    conn = hive.Connection(host='hiveserver2', port=10000, database='default')
    cursor = conn.cursor()
    query = """
        CREATE TABLE IF NOT EXISTS nyc_taxi_clean_dagster AS
        SELECT * FROM nyc_taxi_raw_dagster
        WHERE passenger_count > 0
    """
    cursor.execute(query)
    logger.info("Cleaned data saved in Hive as nyc_taxi_clean_dagster.")
    return "data_cleaned"

@op
def publish_to_kafka(previous_step: str):
    logger = get_dagster_logger()
    producer = Producer({'bootstrap.servers': 'kafka:9092'})
    payload = json.dumps({
        "status": "done",
        "file": HDFS_PATH,
        "timestamp": datetime.now().isoformat()
    })
    producer.produce(KAFKA_TOPIC, key="etl_event", value=payload.encode('utf-8'))
    producer.flush()
    logger.info("Kafka message sent.")
    return "published"

@op
def send_email_notification(previous_step: str):
    logger = get_dagster_logger()
    recipient = os.getenv("DAGSTER_EMAIL_TO")
    if not recipient:
        raise ValueError("Missing DAGSTER_EMAIL_TO environment variable")
    
    msg = EmailMessage()
    msg.set_content(f"El ETL ha terminado correctamente. Archivo cargado en {HDFS_PATH}")
    msg["Subject"] = "[Dagster] ETL completada"
    msg["From"] = os.getenv("DAGSTER_EMAIL")
    msg["To"] = recipient

    with smtplib.SMTP("smtp.gmail.com", 587) as smtp:
        smtp.starttls()
        smtp.login(os.getenv("DAGSTER_EMAIL"), os.getenv("DAGSTER_EMAIL_PASSWORD"))
        smtp.send_message(msg)
    logger.info("Notification email sent.")

@job
def etl_nyc_taxi_hdfs_hive():
    uploaded = upload_to_hdfs()
    table = create_hive_table(uploaded)
    cleaned = clean_data_in_hive(table)
    published = publish_to_kafka(cleaned)
    send_email_notification(published)
