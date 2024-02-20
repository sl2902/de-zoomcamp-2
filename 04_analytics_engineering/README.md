# dbt and BigQuery for Analytics Engineering

![Python](https://img.shields.io/badge/Python-3.8-4B8BBE.svg?style=flat&logo=python&logoColor=FFD43B&labelColor=306998)
![dbt](https://img.shields.io/badge/dbt-1.7-262A38?style=flat&logo=dbt&logoColor=FF6849&labelColor=262A38)
![BigQuery](https://img.shields.io/badge/BigQuery-3772FF?style=flat&logo=googlebigquery&logoColor=white&labelColor=3772FF)
![License](https://img.shields.io/badge/license-CC--BY--SA--4.0-31393F?style=flat&logo=creativecommons&logoColor=black&labelColor=white)

This project focuses on creating dbt models using the NY Taxi Tripdata Datasets in BigQuery. Additionally, it involves developing Dashboards in `Looker Studio` (formerly known as `Google Data Studio`) for data visualizations


## Tech Stack
- [dbt-core](https://github.com/dbt-labs/dbt-core)
- [dbt-bigquery](https://docs.getdbt.com/reference/warehouse-setups/bigquery-setup)
- [Docker](https://docs.docker.com/get-docker/)
- [Pipenv](https://pipenv.pypa.io/en/latest/)
- [Streamlit](https://streamlit.io/)


## Steps to run

**1.** Clone the repository:
```shell
git clone https://github.com/sl2902/de-zoomcamp-2.git
```

**2.** Make sure you have a google project along with Storage and Storage Object Admin role for your account


**3.** Change the working directory:
```shell
cd 04_data_warehouse/
```

**4.** Enable the virtual environment
```shell
pipenv shell
```

**5.** Enable Application Default Credentials
```shell
gcloud auth application-default login
``` 

5.1. Run python script to load data to gcs. Change the argument values for service and year for the other files:

```shell
python py_scripts/load_taxi_trips_to_gcs.py --service=green --year=2019
```

5.2. Run python script to load the parquet files in gcs to bq. Change the argument values for service , table_name, partition_col and clustering_col for the other services:

```shell
python py_scripts/load_gcs_to_bq.py --service=fhv --table_name=stg_fhv_taxi_trips --partition_col=pickup_datetime --clustering_cols=PUlocationID
```

**6.** Update only the gcp project id field in dbt profiles.yml and run the pipeline


6.1. Run `dbt seed` to create the tables from the .csv seed files into the target schema
```shell
dbt seed --select 'taxi_lookup_zone'
```

6.2. Run dbt run to trigger the dbt models to run
```shell
dbt build --vars '{'is_test_run': 'false'}'

# Alternatively you can run only a subset of the models with:

## +models/staging: Runs the dependencies/preceding models first that lead 
## to 'models/staging', and then the target models
dbt [build|run] --select +models/staging

## models/staging+: Runs the target models first, and then all models that depend on it
dbt [build|run] --select models/staging+
```


**7.** Generate the docs:
```shell
dbt docs generate
```

7.1. Serve the docs:
```shell
dbt docs serve --port 8001
```

**8.** Access the generated docs on a web browser at the URL:
```shell
http://localhost:8001
```

**9.** Launch the streamlit app:
[![Open in Streamlit](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://de-zoomcamp-2-kxtdhy8nz5pdvwmjbmyzab.streamlit.app/)