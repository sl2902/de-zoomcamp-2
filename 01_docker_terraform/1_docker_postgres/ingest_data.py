import pandas as pd
import time
import psycopg2
import sqlalchemy
from sqlalchemy import create_engine
from tqdm import tqdm
import argparse
import subprocess
import gzip

url = "postgresql://root:root@localhost:5433/ny_taxi"

def load_trips_data(
        engine: sqlalchemy.Engine, 
        table_name: str,
        src_path: str,
        batch_size: int,
        chunksize: int=1000
):
    try:
        df = pd.read_csv(src_path, compression="gzip", nrows=1)
    except FileNotFoundError as e:
        raise FileNotFoundError
    except Exception as e:
        raise Exception(f"Failed to read file {src_path}")
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists="replace")
    # df_iter = pd.read_csv(src_path, compression="gzip", chunksize=1_000_000, low_memory=False)
    for batch in tqdm(
        pd.read_csv(src_path, compression="gzip", chunksize=chunksize, low_memory=False),
        desc="Processing", 
        total=batch_size,
        leave=True
    ):
        batch.lpep_pickup_datetime = pd.to_datetime(batch.lpep_pickup_datetime)
        batch.lpep_dropoff_datetime = pd.to_datetime(batch.lpep_dropoff_datetime)
        batch.to_sql(name=table_name, con=engine, if_exists='append')

def load_zones_data( 
        engine: sqlalchemy.Engine, 
        table_name: str,
        src_path: str
):
    try:
        df = pd.read_csv(src_path)
    except FileNotFoundError as e:
        raise FileNotFoundError
    except Exception as e:
        raise Exception(f"Failed to read file {src_path}")
    df.to_sql(name=table_name, con=engine, if_exists="replace", index=False)

def q4(engine: sqlalchemy.Engine, table_name: str):
    """
    Which was the pick up day with the largest trip distance Use the pick up time for your calculations.
    """
    query = """
        select
        lpep_pickup_datetime:: date,
        max(trip_distance) largest_trip_distance
        from
        {}
        group by
        lpep_pickup_datetime:: date
        order by
        largest_trip_distance desc
        limit 1
    """.format(table_name)
    print(pd.read_sql(query, con=engine))

def q5(engine: sqlalchemy.Engine, table1: str, table2: str):
    """
    Consider lpep_pickup_datetime in '2019-09-18' and ignoring Borough has Unknown
    Which were the 3 pick up Boroughs that had a sum of total_amount superior to 50000?
    """
    query = """
        select
        "Borough",
        sum(total_amount) total_amt
        from
        {} a join {} b on a."PULocationID"::int = b."LocationID"::int
        where
        "Borough" != 'Unknown'
        and
        lpep_pickup_datetime::date = '2019-09-18'
        group by
        "Borough"
        having
        sum(total_amount) > 50000
        order by
        total_amt desc
        limit 3
    """.format(table1, table2)
    print(pd.read_sql(query, con=engine))

def q6(engine: sqlalchemy.Engine, table1: str, table2: str):
    """
    For the passengers picked up in September 2019 in the zone name Astoria 
    which was the drop off zone that had the largest tip? We want the name of the zone, not the id.
    """
    query = """
        with pickup as (
            select
            "PULocationID",
            "DOLocationID",
            tip_amount
            from
            {table1} a join {table2} b on a."PULocationID" = b."LocationID"
            where
            "Zone" = 'Astoria'
            and
            extract(year from lpep_pickup_datetime::date) = 2019
            and
            extract(month from lpep_pickup_datetime::date) = 9
        ), dropoff as (
            select
            "DOLocationID",
            "Zone",
            max(tip_amount) largest_tip
            from
            pickup a join {table2} b on a."DOLocationID" = b."LocationID"
            group by
            "DOLocationID",
            "Zone"
        )
        select
            "Zone",
            largest_tip
        from
            dropoff
        order by
            largest_tip desc
        limit 1
    """.format(table1=table1, table2=table2)
    print(pd.read_sql(query, con=engine))
    

def main():
    engine = create_engine(url)
    parser = argparse.ArgumentParser()
    parser.add_argument("--src_file1", required=True, help="Enter path to ny taxi trips")
    parser.add_argument("--src_file2", required=True, help="Enter path to zones file")
    parser.add_argument("--table1", help="Enter table name for taxi trip", default="green_taxi_trip")
    parser.add_argument("--table2", help="Enter table name for zones", default="zones")
    args = parser.parse_args()
    # cmd = ["zcat", "<", args.src_file1, "|", "wc", "-l"]
    # out = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    # print(subprocess.list2cmdline(cmd))
    # batch_size = int(out.communicate()[0].decode('utf8').strip().split()[0])
    with gzip.open(args.src_file1, 'rb') as f:
        for i, row in enumerate(f):
            pass
    batch_size = i + 1
    chunksize = 1_000
    batch_size //= chunksize
    load_trips_data(engine, args.table1, args.src_file1, batch_size)
    load_zones_data(engine, args.table2, args.src_file2)
    q4(engine, args.table1)
    q5(engine, args.table1, args.table2)
    q6(engine, args.table1, args.table2)

if __name__ == "__main__":
    main()