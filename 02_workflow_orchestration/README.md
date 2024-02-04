# Mage.ai Workflow Orchestration

![Python](https://img.shields.io/badge/Python-3.10_|_3.11-4B8BBE.svg?style=flat&logo=python&logoColor=FFD43B&labelColor=306998)
![Mage.ai](https://img.shields.io/badge/Mage.ai-0.9-111113?style=flat&logoColor=white&labelColor=111113)
![Docker](https://img.shields.io/badge/Docker-329DEE?style=flat&logo=docker&logoColor=white&labelColor=329DEE)

![License](https://img.shields.io/badge/license-CC--BY--SA--4.0-31393F?style=flat&logo=creativecommons&logoColor=black&labelColor=white)


This module explores using Mage as a workflow orchestration tool to build and end to end pipeline to extract
data from NY green taxi trips for the last three months of 2020, transforms it. The data is exported to both Postgres
and as partitioned parquet files to Google Cloud Storage


## Tech Stack
- [Mage.ai](https://docs.mage.ai/getting-started/setup)
- [Pipenv](https://pipenv.pypa.io/en/latest/)
- [Docker](https://docs.docker.com/get-docker/)


## Steps to run

### Developer Setup (Docker)

**1.** Clone the repository
```shell
git clone https://github.com/sl2902/de-zoomcamp-2.git
```

**2.** Change to working directory:

```shell
cd 02_workflow_orchestration
```

**3.** Create a `.env` file:
```shell
touch .env
```

**3a.** Add the following env variables to it:
```shell
PROJECT_NAME=magic-zoomcamp
POSTGRES_DBNAME=ny_taxi
POSTGRES_SCHEMA=mage
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_HOST=postgres
POSTGRES_PORT=5432

GCP_PROJECT_ID=
GCP_BUCKET=
```

**5.** Create a Google free tier account, and create a service account with appropriate storage access

**6.** Run the Mage pipeline
