import pandas as pd
from google.cloud import bigquery
from google.api_core.exceptions import AlreadyExists, NotFound
import time
from pathlib import Path
import io
import argparse

def create_dataset(project_id: str, dataset_id: str):
    client = bigquery.Client(project=project_id)

    # Set the dataset ID and reference
    dataset_ref = client.dataset(dataset_id)

    # Check if the dataset already exists
    try:
        dataset = client.get_dataset(dataset_ref)
        print(f"Dataset {dataset_id} exists")
    except NotFound:
        print(f"Dataset {dataset_id} not found. Creating dataset")
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"
        dataset = client.create_dataset(dataset)
        print(f"Dataset {dataset_id} has been created")


def load_parquet_to_bq(service: str, table_name: str, partition_col: str, clustering_cols: list):
    client = bigquery.Client()
    if len(clustering_cols) > 0:
        job_config = bigquery.LoadJobConfig(
            create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            source_format=bigquery.SourceFormat.PARQUET,
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field=partition_col,
            ),
            clustering_fields=clustering_cols
        )
    else:
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            source_format=bigquery.SourceFormat.PARQUET,
            time_partitioning=bigquery.TimePartitioning(
                type=bigquery.TimePartitioningType.DAY,
                # field=partition_cols,
            )
        )

    print(job_config.to_api_repr(), table_name)
    load_job = client.load_table_from_uri(
        uri, table_name, job_config=job_config
    )

    load_job.result()

    destination_table = client.get_table(table_name)
    print("Loaded {} rows.".format(destination_table.num_rows))

def main(service: str, table_name: str, partition_col: str="lpep_pickup_datetime", clustering_cols: list=["PULocationID"]):
    start = time.time()
    create_dataset(project_id, dataset)
    load_parquet_to_bq(service, table_name, partition_col, clustering_cols)
    end = time.time()
    print(f"Time to load all paritions of {service} taxi trips {(end-start)} seconds")

if __name__ == "__main__":
    uri = "gs://dbt_taxi_trips_04"
    suffix = "taxitrips"
    project_id = "hive-413217"
    dataset = "taxi_trips_all"
    parser = argparse.ArgumentParser("Load data from GCS to BQ")
    parser.add_argument(
        "--service",
        type=str,
        required=True,
        help="Provide service name. One of fhv, green or yellow"
    )
    parser.add_argument(
        "--table_name",
        type=str,
        required=True,
        help="Provide a table name for the type of service"
    )
    parser.add_argument(
        "--partition_col",
        type=str,
        required=True,
        help="Provide date parition col"
    )
    parser.add_argument(
        "--clustering_cols",
        type=str,
        required=True,
        help="Provide clustering cols separated by comma. For example, \
        if you have multiple clustering columns, it would passed like so - col1,col2"
    )
    args = parser.parse_args()
    service = args.service
    table_name = args.table_name
    table_name = f"{dataset}.{table_name}"
    partition_col = args.partition_col
    uri = f"{uri}/{service}_taxitrips/*"
    try:
        clustering_cols = args.clustering_cols.split(",")
    except Exception as e:
        raise Exception(f"Invalid format {args.clustering_cols} provided for argument --clustering_cols. Check format")

    main(service, table_name, partition_col, clustering_cols)
