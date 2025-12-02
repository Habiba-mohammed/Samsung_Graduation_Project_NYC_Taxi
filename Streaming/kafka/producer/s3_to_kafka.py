import time
import pyarrow.parquet as pq
import pandas as pd
from kafka import KafkaProducer
import boto3
import os
import io
import json

# -----------------------------
# AWS S3 config (use env vars or IAM role/profile)
# -----------------------------
AWS_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
S3_BUCKET = os.environ.get("S3_BUCKET", "uber-real-time-demand-prediction")
S3_KEY = os.environ.get("S3_KEY", "raw/order_items_chunk_1.parquet")

# -----------------------------
# Kafka config
# -----------------------------
KAFKA_BROKER = "kafka:9092"  # container hostname
TOPIC = "s3-taxi-trips"

# -----------------------------
# Initialize S3 client
# -----------------------------
s3_client_kwargs = {"region_name": AWS_REGION}
if AWS_ACCESS_KEY and AWS_SECRET_KEY:
    # If env vars are present, pass them explicitly.
    s3_client_kwargs.update({
        "aws_access_key_id": AWS_ACCESS_KEY,
        "aws_secret_access_key": AWS_SECRET_KEY,
    })

# Create the S3 client. If credentials were not provided, boto3 will fall back
# to the default credential chain (profile, environment, IAM role, etc.).
s3 = boto3.client("s3", **s3_client_kwargs)

# -----------------------------
# Download Parquet file
# -----------------------------
obj = s3.get_object(Bucket=S3_BUCKET, Key=S3_KEY)
data = obj['Body'].read()
table = pq.read_table(io.BytesIO(data))
df = table.to_pandas()

print(f"Loaded {len(df)} rows from S3: {S3_BUCKET}/{S3_KEY}")

# -----------------------------
# Connect to Kafka with retry
# -----------------------------
while True:
    try:
        producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER)
        print(f"Connected to Kafka at {KAFKA_BROKER}")
        break
    except Exception as e:
        print("Kafka not ready, retrying in 5 seconds...")
        time.sleep(5)

# -----------------------------
# Stream rows to Kafka
# -----------------------------
for idx, row in df.iterrows():
    message = row.to_json().encode('utf-8')
    producer.send(TOPIC, message)

    if idx % 100 == 0:
        print(f"Sent row {idx + 1}: {row.to_dict()}")

    time.sleep(0.01) 

producer.flush()
print("Streaming complete!")
