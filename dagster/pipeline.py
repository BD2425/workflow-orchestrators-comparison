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

    drop_table_query = "DROP TABLE IF EXISTS nyc_taxi_raw_dagster"
    cursor.execute(drop_table_query)
    logger.info("Dropped existing Hive table nyc_taxi_raw_dagster if it existed.")

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

    drop_table_query = "DROP TABLE IF EXISTS nyc_taxi_clean_dagster"
    cursor.execute(drop_table_query)
    logger.info("Dropped existing Hive table nyc_taxi_clean_dagster if it existed.")

    query = """
        CREATE TABLE IF NOT EXISTS nyc_taxi_clean_dagster AS
        SELECT * FROM nyc_taxi_raw_dagster
        WHERE passenger_count > 0
    """
    cursor.execute(query)
    logger.info("Cleaned data saved in Hive as nyc_taxi_clean_dagster.")
    return "data_cleaned"

@op
def run_hive_queries(previous_step: str):
    logger = get_dagster_logger()
    conn = hive.Connection(host='hiveserver2', port=10000, database='default')
    cursor = conn.cursor()

    query_1 = """
        SELECT payment_type, COUNT(*) AS num_trips
        FROM nyc_taxi_clean_dagster
        GROUP BY payment_type
        ORDER BY num_trips DESC
    """
    cursor.execute(query_1)
    result_1 = cursor.fetchall()
    logger.info(f"Query 1 (trips by payment type): {result_1}")

    query_2 = """
        SELECT TO_DATE(tpep_pickup_datetime) AS trip_date, COUNT(*) AS num_trips
        FROM nyc_taxi_clean_dagster
        GROUP BY TO_DATE(tpep_pickup_datetime)
        ORDER BY trip_date
    """
    cursor.execute(query_2)
    result_2 = cursor.fetchall()
    logger.info(f"Query 2 (trips by day): {result_2}")

    return {"by_payment_type": result_1, "by_day": result_2}

@op
def publish_to_kafka(hive_results: dict):
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
def send_email_notification(hive_results: dict, _published: str):
    logger = get_dagster_logger()
    recipient = os.getenv("DAGSTER_EMAIL_TO")
    if not recipient:
        raise ValueError("Missing DAGSTER_EMAIL_TO environment variable")
    
    result_1 = hive_results["by_payment_type"]
    result_2 = hive_results["by_day"]

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

    msg = EmailMessage()
    msg.set_content("HTML not supported", subtype='plain')
    msg.add_alternative(content, subtype='html')
    msg["Subject"] = "[Dagster] ETL completada"
    msg["From"] = os.getenv("DAGSTER_EMAIL")
    msg["To"] = recipient

    with smtplib.SMTP("smtp.gmail.com", 587) as smtp:
        smtp.starttls()
        smtp.login(os.getenv("DAGSTER_EMAIL"), os.getenv("DAGSTER_EMAIL_PASSWORD"))
        smtp.send_message(msg)

    logger.info("Notification email with Hive results sent.")

@job
def etl_nyc_taxi_hdfs_hive():
    uploaded = upload_to_hdfs()
    table = create_hive_table(uploaded)
    cleaned = clean_data_in_hive(table)
    hive_results = run_hive_queries(cleaned)
    published = publish_to_kafka(hive_results)
    send_email_notification(hive_results, published)
