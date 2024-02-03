import io
import pandas as pd
import requests
from datetime import datetime

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

taxi_dtypes = {
    "VendorID": pd.Int64Dtype(),
    "passenger_count": pd.Int64Dtype(),
    "trip_distance": float,
    "RatecodeID": pd.Int64Dtype(),
    "store_and_forward_flag": str,
    "PULocationID": pd.Int64Dtype(),
    "DOLocationID": pd.Int64Dtype(),
    "payment_type": pd.Int64Dtype(),
    "fare_amount": float,
    "extra": float,
    "mta_tax": float,
    "tip_amount": float,
    "tolls_amount": float,
    "improvement_surcharge": float,
    "total_amount": float,
    "congestion_surcharge": float,
    # "lpep_pickup_datetime": pd.to_datetime(format="%Y-%m-%d %H:%M:%S"),
    # "lpep_dropoff_datetime": pd.to_datetime(format="%Y-%m-%d %H:%M:%S")
}

parse_dates = ["lpep_pickup_datetime", "lpep_dropoff_datetime"]
@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Template for loading data from API
    """
    file_part = kwargs.get("file_part", "green_tripdata")
    year = kwargs.get("year", "2020")
    ext = kwargs.get("ext", "csv")
    compression = kwargs.get("compression", "gz")
    trip_months = kwargs.get("trip_month", [10, 11, 12])
    df = pd.DataFrame()
    for month in trip_months:
        filename = f"{file_part}_{year}-{month}.{ext}.{compression}"
        url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/{filename}'
        # response = requests.get(url)
        try:
            tmp_df = pd.read_csv(
                url, 
                low_memory=False, 
                dtype=taxi_dtypes, 
                parse_dates=parse_dates,
                date_parser=lambda dt: datetime.strptime(dt, "%Y-%m-%d %H:%M:%S")
            )
        except Exception as e:
            raise Exception(f"Loading data from API failed \n {e}")
        # print(f"month {month}")
        df = pd.concat([df, tmp_df], axis=0)
    print(df.shape)

    return df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
