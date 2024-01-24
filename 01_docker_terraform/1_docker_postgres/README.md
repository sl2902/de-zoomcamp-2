# Python data ingestion with polars and pandas

![Python](https://img.shields.io/badge/Python-3.10_|_3.11-4B8BBE.svg?style=flat&logo=python&logoColor=FFD43B&labelColor=306998)
![Pandas](https://img.shields.io/badge/pandas-150458?style=flat&logo=pandas&logoColor=E70488&labelColor=150458)
![Docker](https://img.shields.io/badge/Docker-329DEE?style=flat&logo=docker&logoColor=white&labelColor=329DEE)

![License](https://img.shields.io/badge/license-CC--BY--SA--4.0-31393F?style=flat&logo=creativecommons&logoColor=black&labelColor=white)

This job loads the NY green taxi trips for September 2019 along with the taxi trips zones dataset into Postgres. A few SQL queries are
run on Postgres to answer a few questions


## Tech Stack
- [pandas](https://pandas.pydata.org/docs/user_guide/)
- [Pipenv](https://pipenv.pypa.io/en/latest/)
- [Docker](https://docs.docker.com/get-docker/)


## Steps to run

### Developer Setup

**1.** Clone the repository
```shell
git clone https://github.com/sl2902/de-zoomcamp-2.git
```

**2.** Create and activate a virtualenv for Python 3.8 using pipenv:
```shell
pipenv shell
```

**3.** Install the dependencies in `Pipfile.lock`:
```shell
pipenv sync
```

**4.** Install the dependencies in `Pipfile.lock`:
```shell
pipenv sync
```

**5.** Run the docker-compose file to load the containers:
```shell
cd 01_docker_postgres
docker-compose up -d
```

**5.** Run the python script:
```shell
python ingest_data.py --src_file1 <green_tripdata_2019-09.csv.gz> --src_file2 <taxi_zone_lookup.csv>
```

