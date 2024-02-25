
import streamlit as st
import pandas as pd
import numpy as np
from google.oauth2 import service_account
from google.cloud import bigquery
import os, sys
import calendar
from datetime import datetime
import pandas_gbq
from time import time
import tqdm


PROJECT_NUMBER = "hive-413217"
DATASET = "dbt_taxi_trips"
TABLE = "part_fact_trips"
FHV_TABLE = "part_fact_fhv_trips"


credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)
client = bigquery.Client(credentials=credentials)
def prepare_txn_query(query):
    print(query)
    # recs = client.query(query).to_dataframe()
    recs = client.query_and_wait(query)
    rows = [dict(row) for row in recs]
    return pd.DataFrame(rows)

def query_bq(query):
    print(query)
    return pandas_gbq.read_gbq(query)

# def generate_all_rows(qry, table, years=[2019, 2020]):
#     df = pd.DataFrame()
#     for year in years:
#         for month in range(1, 13):
#             start = time()
#             first, last = calendar.monthrange(year, month)
#             start_date = datetime.strftime(datetime(year, month, first), '%Y-%m-%d')
#             end_date = datetime.strftime(datetime(year, month, last), '%Y-%m-%d')
#             tmp_df = query_bq(qry.format(project_number=PROJECT_NUMBER, bq_dataset=DATASET, table=table, start_date=start_date, end_date=end_date)
#             )
#             df = pd.concat([tmp_df, df])
#             end = time()
#         print(f"{year}-{month} took {end-start} seconds")
#     return df

def generate_all_rows(qry, table, partitions):
    df = pd.DataFrame()
    for partition in partitions:
        start = time()
        tmp_df = query_bq(qry.format(project_number=PROJECT_NUMBER, bq_dataset=DATASET, table=table, partition=partition)
        )
        df = pd.concat([tmp_df, df])
        end = time()
        print(f"{partition} took {end-start} seconds")
    return df

def list_partitions(project_number, dataset, table, start_date:int, end_date:int):
    client = bigquery.Client()
    part_ids = sorted([int(pid) for pid in client.list_partitions(f"{project_number}.{dataset}.{table}")])
    start = part_ids.index(start_date)
    end = part_ids.index(end_date)
    
    return part_ids[start: end+1]

qry1 = """
        SELECT
            trip_id,
            vendor_id,
            service_type,
            pickup_locationid,
            pickup_borough,
            pickup_zone,
            dropoff_borough,
            dropoff_zone,
            pickup_datetime,
            dropoff_datetime,
            passenger_count,
            trip_distance,
            total_amount,
            payment_type_description,
            extract(year from pickup_datetime) as year,
            format_date('%B', pickup_datetime) as month
        FROM
            `{project_number}.{bq_dataset}.{table}`
        WHERE
            format_date('%Y%m%d', extract(date from pickup_datetime)) = '{partition}'
        AND
            total_amount > 0
        AND
            total_amount < 1000
        """

qry2 = """
        SELECT
            Affiliated_base_number,
            'fhv' as service_type,
            pickup_locationid,
            pickup_borough,
            pickup_zone,
            dropoff_borough,
            dropoff_zone,
            pickup_datetime,
            dropoff_datetime,
            extract(year from pickup_datetime) as year,
            format_date('%B', pickup_datetime) as month
        FROM
            `{project_number}.{bq_dataset}.{table}`
        WHERE
            cast(extract(date from pickup_datetime) as date) >= '{start_date}'
        AND
            cast(extract(date from pickup_datetime) as date) <= '{end_date}'
        AND
            Affiliated_base_number is not null
        
        """
partitions = list_partitions(PROJECT_NUMBER, DATASET, TABLE, 20190101, 20201231)
df = generate_all_rows(qry1, TABLE, partitions)
print(df.shape)

# df2 = generate_all_rows(qry2, FHV_TABLE, years=[2019])
# print(len(list_partitions(PROJECT_NUMBER, DATASET, TABLE)))