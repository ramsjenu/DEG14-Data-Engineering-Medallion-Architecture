import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator

from kafka import KafkaConsumer
import json
from pathlib import Path
import boto3
from botocore.client import Config
import pandas as pd
from datetime import datetime, timedelta
import io

aws_access_key_id="minio"
aws_secret_access_key="minio123"

s3 = boto3.client('s3', endpoint_url="http://minio:9000", aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, config=Config(signature_version='s3v4'))

def get_buckets_list():
    bucket_list = []
    response = s3.list_buckets()
    if 'Buckets' in response:
        for bucket in response['Buckets']:
            bucket_list.append(bucket['Name'])
        return bucket_list
    else:
        print("No buckets found.")
        return None        

# List all objects inside the bucket
def list_objects_in_bucket(bucket):
    file_list = []
    response = s3.list_objects_v2(Bucket=bucket)
    if 'Contents' in response:
        for obj in response['Contents']:
            print(f"Bucket: {bucket} Object: {obj['Key']}")
            file_list.append(obj['Key'])
        return file_list
    else:
        print(f"No objects found in {bucket}")
        return file_list

def check_folder(bucket, file_name):
    bucket_obj_list = list_objects_in_bucket(bucket)

    for bucket_objs in bucket_obj_list:
        if file_name not in bucket_objs:
            pass
        else:
            print(f"{file_name} already exist")
            return True
    return False

# Create a folder (S3 treats folders as objects with '/' at the end)
def create_folder(bucket, folder_name):
    folder_key = f"{folder_name}/"
    s3.put_object(Bucket=bucket, Key=folder_key)
    print(f"Folder '{folder_name}' created successfully.")

def convert_timestamps(df):
    # Convert pickup and dropoff timestamps from milliseconds to datetime
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'], unit='ms')
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'], unit='ms')
    return df

def clean_data(df):
    # Fill missing numerical values with the median or other relevant strategy
    df['RatecodeID'].fillna(df['RatecodeID'].median(), inplace=True)
    df['passenger_count'].fillna(df['passenger_count'].median(), inplace=True)
    df['payment_type'].fillna(df['payment_type'].mode()[0], inplace=True)  # Use mode for categorical data
    df['congestion_surcharge'].fillna(0, inplace=True)  # Fill congestion surcharge with 0 (default assumption)

    # Fill missing categorical values with the most frequent value or a default
    df['store_and_fwd_flag'].fillna('N', inplace=True)  # Assuming 'N' (no store and forward) is a safe default

    # Drop any duplicates that may have been introduced
    df.drop_duplicates(inplace=True)

    # Ensure there are no remaining missing values
    df.fillna(0, inplace=True)  # Final fallback to fill any remaining NaNs with 0 (for numeric columns)
    df = convert_timestamps(df)
    return df


def push_data_to_silver_layer(bucket_name, file_path):
    # List objects in the bucket
    response = s3.list_objects_v2(Bucket=bucket_name)

    # Loop over the objects
    if 'Contents' in response:
        for obj in response['Contents']:
            object_name = obj['Key']
            print(f"Found: {object_name} in {bucket_name}")

            if 'data' in object_name:
                # Generate a presigned URL for downloading the object
                url = s3.generate_presigned_url(
                    'get_object',
                    Params={
                        'Bucket': bucket_name,
                        'Key': object_name
                    },
                    ExpiresIn=3600  # URL expires in 1 hour
                )

                # Read the parquet file using the URL
                data = pd.read_parquet(url)
                print(f"Reading: {object_name}")

                for index, row in data.iterrows():
                    vendor_id = str(row['VendorID'])
                    pickup_datetime = str(row['tpep_pickup_datetime'])
                    folder_name = pickup_datetime.split(" ")[0]
                    
                    pickup_datetime_formated = pickup_datetime.replace(":", "-").replace(" ", "_")
                    file_name = f"trip_{vendor_id}_{pickup_datetime_formated}.json"

                    # Convert the row to JSON
                    record = row.to_json()
                    record_bytes = record.encode('utf-8')
                    record_stream = io.BytesIO(record_bytes)

                    # Upload the JSON data to a new S3 bucket
                    s3.put_object(
                        Bucket='gold',
                        Key=f"{Path(folder_name).stem}/{file_name}",
                        Body=record_stream,
                        ContentType='application/json'
                    )

                    print(f"Uploaded {file_name} to S3.")


def consume_data():
    # Initialize Kafka consumer
    consumer = KafkaConsumer(
        'silver_layer_data',
        bootstrap_servers=['broker:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
       
       
    for message in consumer:
        data = message.value
        if 'Key' in data:
            file_name = Path(data['Key']).stem
            push_data_to_silver_layer("silver", data["Key"])
        else:
            print("Key not found in message.")
        
        
dag = DAG(
    dag_id = "3_gold_layer_processing",
    default_args = {
        "owner" : "Prabakar",
        "start_date" : airflow.utils.dates.days_ago(1),
    },
    schedule_interval = "@yearly",
    catchup = False
)

start = PythonOperator(
    task_id = "start",
    python_callable = lambda: print("Jobs Started"),
    dag=dag
)

gold_data_consumer = PythonOperator(
    task_id="gold_data_consumer",
    python_callable=consume_data,
    dag=dag
)

end = PythonOperator(
    task_id="end",
    python_callable = lambda: print("Jobs completed successfully"),
    dag=dag
)

start >> gold_data_consumer >> end
