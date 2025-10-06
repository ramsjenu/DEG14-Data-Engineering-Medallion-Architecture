import io
import time
import pandas as pd
from datetime import datetime, timedelta
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
    """List all objects in a given S3 bucket."""
    s3 = get_s3_client()
    response = s3.list_objects_v2(Bucket=bucket)
    if "Contents" in response:
        return [obj["Key"] for obj in response["Contents"]]
    else:
        return []


def ensure_bucket_exists(bucket_name):
    """Ensure target bucket exists."""
    s3 = get_s3_client()
    try:
        s3.create_bucket(Bucket=bucket_name)
        print(f"✅ Bucket '{bucket_name}' created.")
    except s3.exceptions.BucketAlreadyOwnedByYou:
        pass
    except Exception as e:
        print(f"⚠️ Could not create bucket '{bucket_name}': {e}")


# ---------------------------------------------------------------------
# Core Gold Logic (Streaming Style)
# ---------------------------------------------------------------------

def write_partitioned_gold(df, gold_bucket):
    """Write data partitioned by date into the gold bucket."""
    s3 = get_s3_client()

    df["date"] = df["tpep_pickup_datetime"].dt.date

    for date_val, group_df in df.groupby("date"):
        folder_path = f"date={date_val}"
        print(f"📦 Writing data for {folder_path} ({len(group_df)} rows)")

        output_stream = io.BytesIO()
        group_df.to_parquet(output_stream, index=False)
        output_stream.seek(0)

        key = f"{folder_path}/part-{int(datetime.utcnow().timestamp())}.parquet"

        s3.put_object(
            Bucket=gold_bucket,
            Key=key,
            Body=output_stream.getvalue(),
            ContentType="application/octet-stream",
        )
        print(f"✅ Uploaded {key} to {gold_bucket}")


def gold_streaming_job():
    """Continuously process new silver data into gold layer."""
    s3 = get_s3_client()
    silver_bucket = "silver"
    gold_bucket = "gold"

    ensure_bucket_exists(gold_bucket)
    processed_files = set()  # to track what’s already processed

    print("♻️ Gold streaming job started... continuously watching silver layer.")

    while True:
        silver_files = list_objects_in_bucket(silver_bucket)
        if not silver_files:
            print("⏳ No silver files found yet, waiting 15s...")
            time.sleep(15)
            continue

        for key in silver_files:
            if not key.endswith(".parquet"):
                continue
            if key in processed_files:
                continue  # skip already processed

            print(f"📥 New silver file detected: {key}")
            file_stream = io.BytesIO()
            s3.download_fileobj(silver_bucket, key, file_stream)
            file_stream.seek(0)

            df = pd.read_parquet(file_stream)
            df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])

            # Write partitioned output
            write_partitioned_gold(df, gold_bucket)

            processed_files.add(key)
            print(f"✅ {key} processed successfully to gold.")

        print("🔁 Checking for new files again in 30s...")
        time.sleep(30)  # polling interval


# ---------------------------------------------------------------------
# Airflow DAG Definition
# ---------------------------------------------------------------------

default_args = {
    "owner": "Prabakar",
    "start_date": datetime(2025, 10, 5),
    "retries": 0,
}

with DAG(
    dag_id="gold_layer_streaming",
    default_args=default_args,
    schedule=None,  # no cron, runs continuously
    catchup=False,
    tags=["gold", "streaming", "data-pipeline"],
) as dag:

    start = PythonOperator(
        task_id="start",
        python_callable=lambda: print("🚀 Gold layer streaming started"),
    )

    gold_consumer = PythonOperator(
        task_id="gold_consumer",
        python_callable=gold_streaming_job,
    )

    end = PythonOperator(
        task_id="end",
        python_callable=lambda: print("✅ Gold layer streaming ended"),
    )

    start >> gold_consumer >> end
