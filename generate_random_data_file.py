import random
import json
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from faker import Faker
from datetime import datetime, timedelta

# Initialize Faker
fake = Faker()

def generate_timestamp():
    start = datetime.now() - timedelta(days=10)
    end = datetime.now()
    timestamp = fake.date_time_between(start_date=start, end_date=end)
    return int(timestamp.timestamp() * 1000)

def generate_data(num_records):
    data_list = []
    
    for i in range(num_records):
        record = {
            "VendorID": random.choice([1, 2, 3, 4, 5]),
            "tpep_pickup_datetime": generate_timestamp(),
            "tpep_dropoff_datetime": generate_timestamp(),
            "passenger_count": round(random.uniform(1, 6), 1),
            "trip_distance": round(random.uniform(0.5, 20.0), 2),
            "RatecodeID": random.choice([1.0, 2.0, 3.0, None]),  # Sometimes missing
            "store_and_fwd_flag": random.choice(["Y", "N", None]),  # Sometimes missing
            "PULocationID": random.randint(1, 265),
            "DOLocationID": random.randint(1, 265),
            "payment_type": random.choice([1, 2, 3, 4, None]),  # Sometimes missing
            "fare_amount": round(random.uniform(5.0, 100.0), 2),
            "extra": round(random.uniform(0.5, 10.0), 2),
            "mta_tax": 0.5,
            "tip_amount": round(random.uniform(0.0, 20.0), 2),
            "tolls_amount": round(random.uniform(0.0, 10.0), 2),
            "improvement_surcharge": 1.0,
            "total_amount": round(random.uniform(10.0, 150.0), 2),
            "congestion_surcharge": random.choice([0.0, 2.5, None]),  # Sometimes missing
            "Airport_fee": random.choice([0.0, 5.0])
        }

        # Introduce some duplicate values
        if random.random() < 0.1:
            record["PULocationID"] = record["DOLocationID"]

        data_list.append(record)
        print(f"{i} {record}")

    return data_list

def save_to_parquet(data_list, file_name="data.parquet"):
    df = pd.DataFrame(data_list)
    table = pa.Table.from_pandas(df)
    pq.write_table(table, file_name)

# Generate dataset and save to parquet
num_records = int(input("Enter the number of records to generate: "))
data = generate_data(num_records)
save_to_parquet(data)

print(f"{num_records} records generated and saved to data.parquet")
