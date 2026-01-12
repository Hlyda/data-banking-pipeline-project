import boto3
from kafka import KafkaConsumer
import json
import pandas as pd
from datetime import datetime
import os
from dotenv import load_dotenv
import logging
import atexit

# Load environment variables from .env
load_dotenv()

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)

# Kafka topics to consume
KAFKA_TOPICS = [
    'banking_server.public.customers',
    'banking_server.public.accounts',
    'banking_server.public.transactions'
]

# Batch size for MinIO uploads
BATCH_SIZE = 50

# Kafka and MinIO configuration from environment variables
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP")
KAFKA_GROUP = os.getenv("KAFKA_GROUP")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")

# Kafka consumer setup
consumer = KafkaConsumer(
    *KAFKA_TOPICS,
    bootstrap_servers=KAFKA_BOOTSTRAP,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id=KAFKA_GROUP,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# MinIO client setup
s3 = boto3.client(
    's3',
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY
)

def extract_record(event):
    """
    Extract the actual record from a Kafka message.
    Supports Debezium-like payload or raw message.
    """
    if "payload" in event and "after" in event["payload"]:
        return event["payload"]["after"]
    return event

def write_to_minio(table_name, records):
    """
    Write a batch of records to MinIO in Parquet format.
    """
    if not records:
        return

    df = pd.DataFrame(records)
    date_str = datetime.now().strftime('%Y-%m-%d')
    file_path = f'{table_name}_{date_str}.parquet'
    df.to_parquet(file_path, engine='fastparquet', index=False)

    s3_key = f'{table_name}/date={date_str}/{table_name}_{datetime.now().strftime("%H%M%S%f")}.parquet'

    try:
        if MINIO_BUCKET not in [b['Name'] for b in s3.list_buckets()['Buckets']]:
            s3.create_bucket(Bucket=MINIO_BUCKET)
        s3.upload_file(file_path, MINIO_BUCKET, s3_key)
        logging.info(f"Uploaded {len(records)} records to s3://{MINIO_BUCKET}/{s3_key}")
    except Exception as e:
        logging.error(f"Failed to upload {file_path} to MinIO: {e}")
    finally:
        if os.path.exists(file_path):
            os.remove(file_path)

def consume_and_write():
    buffer = {topic: [] for topic in KAFKA_TOPICS}
    logging.info("Connected to Kafka. Listening for messages...")

    try:
        for message in consumer:
            topic = message.topic
            event = message.value
            record = extract_record(event)

            if record:
                buffer[topic].append(record)
                logging.info(f"[{topic}] -> {record}")

            if len(buffer[topic]) >= BATCH_SIZE:
                write_to_minio(topic.split('.')[-1], buffer[topic])
                buffer[topic] = []

    except Exception as e:
        logging.error(f"Error while consuming Kafka messages: {e}")

def flush_buffers():
    logging.info("Flushing remaining records before exit...")
    for topic, records in buffer.items():
        if records:
            write_to_minio(topic.split('.')[-1], records)

# Register flush function to ensure all records are written on exit
buffer = {topic: [] for topic in KAFKA_TOPICS}  # global buffer for atexit
atexit.register(flush_buffers)

if __name__ == "__main__":
    consume_and_write()
