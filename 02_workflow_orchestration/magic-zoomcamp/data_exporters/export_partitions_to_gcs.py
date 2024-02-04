import pyarrow as pa 
import pyarrow.parquet as pq 
import os

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data(data, *args, **kwargs):
    """
    Exports data to some source.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Output (optional):
        Optionally return any object and it'll be logged and
        displayed when inspecting the block run.
    """
    bucket = os.environ.get("GCP_BUCKET")
    folder = kwargs.get("file_part")
    filepath = f"{bucket}/{folder}"
    # project_id = os.environ.get("GCP_PROJECT_ID")
    fs_gcs = pa.fs.GcsFileSystem()
    print(f'num partitions {data["lpep_pickup_date"].nunique()}')
    # os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/src/personal-gcp.json"
    # Specify your data exporting logic here
    table = pa.Table.from_pandas(data)
    pq.write_to_dataset(
        table, 
        root_path=filepath, 
        filesystem=fs_gcs, 
        partition_cols=['lpep_pickup_date'],
        compression='snappy'
    )



