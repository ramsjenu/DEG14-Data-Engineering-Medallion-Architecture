import io
import json
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from kafka import KafkaConsumer
import boto3
from botocore.client import Config
from airflow import DAG
from airflow.operators.python import PythonOperator


# ---------------------------------------------------------------------
# Utility Functions
# ---------------------------------------------------------------------

def get_s3_client():
    """Return a boto3 S3 client configured for MinIO."""
    return boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="admin",
        aws_secret_access_key="password",
        config=Config(signature_version="s3v4"),
    )


def list_objects_in_bucket(bucket):
    """List all objects in a given bucket."""
    s3 = get_s3_client()
    response = s3.list_objects_v2(Bucket=bucket)
    file_list = []

    if "Contents" in response:
        for obj in response["Contents"]:
            print(f"Bucket: {bucket} Object: {obj['Key']}")
            file_list.append(obj["Key"])
    else:
        print(f"No objects found in {bucket}")
    return file_list


def check_folder_and_create(bucket, file_name):
    """Check if a file exists in a bucket."""
    bucket_obj_list = list_objects_in_bucket(bucket)
    for bucket_objs in bucket_obj_list:
        if file_name in bucket_objs:
            print(f"{file_name} already exists in {bucket}")
            return True
    return False


def convert_timestamps(df):
    """Convert pickup and dropoff timestamps from milliseconds to datetime."""
    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"], unit="ms")
    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"], unit="ms")
    return df


def clean_data(df):
    """Clean and standardize data."""
    df["RatecodeID"].fillna(df["RatecodeID"].median(), inplace=True)
    df["passenger_count"].fillna(df["passenger_count"].median(), inplace=True)
    df["payment_type"].fillna(df["payment_type"].mode()[0], inplace=True)
    df["congestion_surcharge"].fillna(0, inplace=True)
    df["store_and_fwd_flag"].fillna("N", inplace=True)
    df.drop_duplicates(inplace=True)
    df.fillna(0, inplace=True)
    return convert_timestamps(df)


def push_data_to_silver_layer(bucket_name, file_path):
    """Process all parquet files in the bronze layer and upload cleaned versions to silver."""
    s3 = get_s3_client()
    response = s3.list_objects_v2(Bucket=bucket_name)

    if "Contents" not in response:
        print(f"No objects found in {bucket_name}")
        return

    for obj in response["Contents"]:
        object_name = obj["Key"]

        # Only process relevant files
        if "data" not in object_name:
            continue

        print(f"Processing {object_name} from {bucket_name}")

        # Download object into memory (BytesIO)
        file_stream = io.BytesIO()
        s3.download_fileobj(bucket_name, object_name, file_stream)
        file_stream.seek(0)

        # Read parquet into DataFrame
        df = pd.read_parquet(file_stream)
        print(f"Loaded {len(df)} rows from {object_name}")

        # Clean and transform data
        cleaned_df = clean_data(df)

        # Convert back to Parquet in memory
        output_stream = io.BytesIO()
        cleaned_df.to_parquet(output_stream, index=False)
        output_stream.seek(0)

        # Upload cleaned data to the silver bucket
        s3.put_object(
            Bucket="silver",
            Key=f"{object_name}",
            Body=output_stream.getvalue(),
            ContentType="application/octet-stream",
        )

        print(f"✅ Uploaded cleaned {object_name} to silver layer.")


def consume_data():
    """Consume Kafka messages and process new bronze files."""
    consumer = KafkaConsumer(
        "bronze_layer_data",
        bootstrap_servers=["broker:9092"],
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )

    for message in consumer:
        data = message.value
        print(f"Received message: {data}")

        if "Key" not in data:
            print("Key not found in message; skipping.")
            continue

        file_name = Path(data["Key"]).stem
        print(f"Processing file: {file_name}")

        # Check if file already exists in the silver bucket
        if not check_folder_and_create(bucket="silver", file_name=file_name):
            push_data_to_silver_layer("bronze", data["Key"])
        else:
            print(f"{file_name} already exists in silver layer.")


# ---------------------------------------------------------------------
# Airflow DAG Definition
# ---------------------------------------------------------------------

default_args = {
    "owner": "Prabakar",
    "start_date": datetime(2025, 10, 5),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="silver_layer_batch_processing",
    default_args=default_args,
    schedule="@yearly",
    catchup=False,
    tags=["silver", "data-pipeline"],
) as dag:

    start = PythonOperator(
        task_id="start",
        python_callable=lambda: print("🚀 Silver layer processing started"),
    )

    process_bronze_files = PythonOperator(
        task_id="process_bronze_files",
        python_callable=consume_data,
    )

    end = PythonOperator(
        task_id="end",
        python_callable=lambda: print("✅ Silver layer processing completed successfully"),
    )

    start >> process_bronze_files >> end
