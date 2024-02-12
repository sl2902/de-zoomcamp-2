import pandas as pd
import requests
import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage
from google.api_core.exceptions import AlreadyExists, Conflict
import time
from pathlib import Path
import io

base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data"
filename = "green_tripdata"
# filename_example = "green_tripdata_2020-01.parquet"

def create_bucket(bucket_name: str):
    """Create bucket"""
    client = storage.Client()
    try:
        bucket = client.get_bucket(bucket_name)
        print(f"Bucket {bucket_name} already exists")
    except:
        try:
            bucket = client.bucket(bucket_name)
            bucket.storage_class = "STANDARD"
            new_bucket = client.create_bucket(bucket, location="us")
            print(f"Bucket {bucket_name} has been created")
        except Conflict as e:
            print(f"Bucket {bucket_name} namespace should be unique. Pick another name")
            raise e
    return bucket_name
    
def read_external_data(year, month) -> pd.DataFrame:
    """Read parquet files for the specific year from url"""
    url = f"{base_url}/{filename}_{year}-{month:02d}.parquet"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
    }   
    df = pd.read_parquet(url)
    # try:
    #     response = requests.get(url, headers=headers)
    # except Exception as e:
    #     raise e
    
    # with open(f"../input_data/{filename}_{year}-{month:02d}.parquet", "wb") as f:
    #     f.write(response.content)
    return df

def transform_df(df: pd.DataFrame, year: int, month: int) -> pa.Table:
    """Change dtype of a few fields. Convert df to pyarrow table"""
    # to be used for partition
    # df["lpep_pickup_date"] = df["lpep_pickup_datetime"].dt.date
    df["year"] = str(year)
    df["month"] = f"{month:02d}"
    schema = pa.schema([
        ("VendorID", pa.int64()),
        ("lpep_pickup_datetime", pa.timestamp('us')),
        ("lpep_dropoff_datetime", pa.timestamp('us')),
        ("passenger_count", pa.int32()),
        ("RatecodeID", pa.int32()),
        ("PULocationID", pa.int32()),
        ("DOLocationID", pa.int32()),
        ("payment_type", pa.int32()),
        ("store_and_fwd_flag", pa.string()),
        ("tolls_amount", pa.float32()),
        ("improvement_surcharge", pa.float32()),
        ("congestion_surcharge", pa.float32()),
        ("mta_tax", pa.float32()),
        ("tip_amount", pa.float32()),
        ("extra", pa.float32()),
        ("trip_distance", pa.float64()),
        ("fare_amount", pa.float64()),
        ("ehail_fee", pa.string()),
        ("trip_type", pa.float32()),
        ("total_amount", pa.float64()),
        # ("lpep_pickup_date", pa.date32())
        ("year", pa.string()),
        ("month", pa.string())
    ])

    table = pa.Table.from_pandas(df, schema=schema)
    return table

def upload_to_gcs(bucket_name: str, object_name: str, table: pa.Table, partition_cols: list):
    """Upload parquet file to gcs"""
    fs_gcs = pa.fs.GcsFileSystem()
    filepath = f"{bucket_name}/{object_name}"

    pq.write_to_dataset(
        table, 
        root_path=filepath, 
        filesystem=fs_gcs, 
        partition_cols=partition_cols,
        compression='snappy',
        # basename_template="guid-{i}.parquet",
        existing_data_behavior="delete_matching"
    )


def main(bucket_name: str, object_name: str, year: int=2022, month: int=1):
    start_time = time.time()
    create_bucket(bucket_name)
    for yy in range(year, year+1):
        for mm in range(1, 13):
            df = read_external_data(yy, mm)
            table = transform_df(df, yy, mm)
            upload_to_gcs(bucket_name, object_name, table, ["year", "month"])
            print(f"Loaded partition year={yy}/month={mm}")
    end_time = time.time()
    print(f"Time to complete uploading files for year-{year} is {(end_time-start_time)/60} minutes")

if __name__ == "__main__":
    bucket_name = "taxi_trips_03"
    object_name = "green_taxitrips"
    main(bucket_name, object_name)