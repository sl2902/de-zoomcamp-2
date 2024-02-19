import pyarrow as pa
import pyarrow.parquet as pq

schema = pa.schema([
            ("VendorID", pa.int64()),
            # ("lpep_pickup_datetime", pa.timestamp('us')),
            # ("lpep_dropoff_datetime", pa.timestamp('us')),
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
            ("ehail_fee", pa.float64()),
            ("trip_type", pa.float32()),
            ("total_amount", pa.float64()),
            # ("lpep_pickup_date", pa.date32())
            # ("year", pa.string()),
            # ("month", pa.string())
])