import pyarrow.parquet as pq

# Specify the GCS path to the partitioned Parquet file
gcs_path = "gs://dbt_taxi_trips_04/green_taxitrips/year=2020/month=01/"

# Read the partitioned Parquet file into a PyArrow Table
table = pq.read_table(gcs_path)

# Convert the PyArrow Table to a Pandas DataFrame
df = table.to_pandas()

# Display the DataFrame
print(df.dtypes)