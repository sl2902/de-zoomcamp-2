# PySpark Batch processing

![Python](https://img.shields.io/badge/Python-3.8_-4B8BBE.svg?style=flat&logo=python&logoColor=FFD43B&labelColor=306998)
![PySpark](https://img.shields.io/badge/pySpark-3.5.1-E36B22?style=flat-square&logo=apachespark&logoColor=E36B22&labelColor=3C3A3E)
![Jupyter](https://img.shields.io/badge/Jupyter-31393F.svg?style=flat&logo=jupyter&logoColor=F37726&labelColor=31393F)
![Docker](https://img.shields.io/badge/Docker-329DEE?style=flat&logo=docker&logoColor=white&labelColor=329DEE)

![License](https://img.shields.io/badge/license-CC--BY--SA--4.0-31393F?style=flat&logo=creativecommons&logoColor=black&labelColor=white)

This module explores using PySpark to process NYC Taxi trip data for FHV October 2019


## Tech Stack
- [PySpark](https://spark.apache.org/docs/latest/api/python/user_guide)
- [Docker](https://docs.docker.com/get-docker/)
- [Pipenv](https://pipenv.pypa.io/en/latest/)


## Steps to run

**1.** Install `JDK` 11 or 17, Spark 3.5.x, and Hadoop:

```shell
sdk i java 17.0.10-librca
sdk i spark 3.5.1
sdk i hadoop 3.3.5
```

**2.** Clone the repository:
```shell
git clone https://github.com/sl2902/de-zoomcamp-2.git
```

**3.** Change the working directory
```shell
cd 05_batch_processing/
```

**4.** Enable the virtual environment:
```shell
pipenv shell
```

**5.** Run jupyter notebook:
```shell
pipenv run jupyter notebook
```