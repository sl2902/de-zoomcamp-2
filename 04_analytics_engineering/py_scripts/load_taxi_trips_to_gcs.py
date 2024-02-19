import pandas as pd
import numpy as np
# import requests
import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage
from google.api_core.exceptions import AlreadyExists, Conflict
import time
from pathlib import Path
import io
import argparse
from taxi_trip_arrow_schema import schema

base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data"
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
    try:
        df = pd.read_parquet(url)
    except Exception as e:
        raise Exception(f"Failed to read parquet file from {url}")
    # try:
    #     response = requests.get(url, headers=headers)
    # except Exception as e:
    #     raise e
    
    # with open(f"../input_data/{filename}_{year}-{month:02d}.parquet", "wb") as f:
    #     f.write(response.content)
    return df

def transform_df(service: str, schema: pa.schema, df: pd.DataFrame, year: int, month: int) -> pa.Table:
    """Change dtype of a few fields. Convert df to pyarrow table"""
    # to be used for partition
    # df["lpep_pickup_date"] = df["lpep_pickup_datetime"].dt.date
    df["year"] = str(year)
    df["month"] = f"{month:02d}"
    if service == "yellow":
        # ehail_fee and trip_type are not present in yello_taxitrips
        if "ehail_fee" not in df.columns:
            df["ehail_fee"] = np.nan
        if "trip_type" not in df.columns:
            df["trip_type"] = 1
    if service == "green":
        schema = schema.insert(1, pa.field("lpep_pickup_datetime", pa.timestamp('us')))
        schema = schema.insert(2, pa.field("lpep_dropoff_datetime", pa.timestamp('us')))
    elif service == "yellow":
        schema = schema.insert(1, pa.field("tpep_pickup_datetime", pa.timestamp('us')))
        schema = schema.insert(2, pa.field("tpep_dropoff_datetime", pa.timestamp('us')))
    elif service == "fhv":
        schema = pa.schema([
            ("dispatching_base_num", pa.string()),
            ("pickup_datetime", pa.timestamp('us')),
            ("dropOff_datetime", pa.timestamp('us')),
            ("PUlocationID", pa.int32()),
            ("DOlocationID", pa.int32()),
            ("SR_Flag", pa.uint8()),
            ("Affiliated_base_number", pa.string())
        ])
    else:
        raise Exception(f"Invalid service option - {service}")
    
    schema = schema.append(
            pa.field("year", pa.string())
    )
    schema = schema.append(
            pa.field("month", pa.string())
    )


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


def main(service: str, schema: pa.schema, bucket_name: str, object_name: str, year: int=2022, month: int=1):
    start_time = time.time()
    create_bucket(bucket_name)
    for yy in range(year, year+1):
        for mm in range(month, month + 12):
            df = read_external_data(yy, mm)
            table = transform_df(service, schema, df, yy, mm)
            upload_to_gcs(bucket_name, object_name, table, ["year", "month"])
            print(f"Loaded {service} partition year={yy}/month={mm}")
    end_time = time.time()
    print(f"Time to complete uploading files for year-{year} is {(end_time-start_time)/60} minutes")

if __name__ == "__main__":
    bucket_name = "dbt_taxi_trips_04"
    parser = argparse.ArgumentParser('Load taxi trips parquet files to gcs')
    parser.add_argument(
        "--service",
        type=str,
        required=True,
        help="Provide service name for taxi trips - fhv, green or yellow"
    )
    parser.add_argument(
        "--year",
        type=int,
        required=True,
        help="Provide year of data to download. Format yyyy"
    )
    args = parser.parse_args()
    service = args.service
    year = args.year
    object_name = f"{service}_taxitrips"
    filename = f"{service}_tripdata"
    main(service, schema, bucket_name, object_name, year)