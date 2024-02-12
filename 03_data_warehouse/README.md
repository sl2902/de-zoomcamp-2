# Data Warehouse - Bigquery

![BigQuery](https://img.shields.io/badge/BigQuery-3772FF?style=flat&logo=googlebigquery&logoColor=white&labelColor=3772FF)
![GCP](https://img.shields.io/badge/Google_Cloud-3772FF?style=flat&logo=googlecloud&logoColor=white&labelColor=3772FF)

![License](https://img.shields.io/badge/license-CC--BY--SA--4.0-31393F?style=flat&logo=creativecommons&logoColor=black&labelColor=white)

This module explores using Biqquery as a warehouse to process NY green taxi trips for 2022. Parquet files
are loaded into Google Cloud Storage, and various tables - native, external, parititoned tables are created
to check the performance of queries in terms of bytes processed


## Tech Stack
[Pipenv](https://pipenv.pypa.io/en/latest/)
[Python](https://www.python.org/)
[GCS](https://cloud.google.com/storage?hl=en)
[BQ](https://cloud.google.com/bigquery?hl=en)

### Steps to run

**1.** Clone the repo

```shell
git clone https://github.com/sl2902/de-zoomcamp-2.git
```

**2.** Make sure you have a google project along with Storage and Storage Object Admin role for your account

**3.** Change working directory
```shell
cd 03_data_warehouse/
```

**3.** Enable the virtual environment
```shell
pipenv shell
```

**4.** Run the following script to load the parquet files to GCS
```shell
python load_nyc_taxi_trips_to_gcs.py
```

**5.** Once completed, head to Bigquery UI. Create a dataset

**6.** Use the various queries in the `bq_tables.sql` file to create various types of tables and also run the queries under each question

