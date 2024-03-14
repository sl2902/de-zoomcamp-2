# PySpark Streaming

![Python](https://img.shields.io/badge/Python-3.8-4B8BBE.svg?style=flat&logo=python&logoColor=FFD43B&labelColor=306998)
![PySpark](https://img.shields.io/badge/pySpark-3.3-E36B22?style=flat-square&logo=apachespark&logoColor=E36B22&labelColor=3C3A3E)
![Jupyter](https://img.shields.io/badge/Jupyter-31393F.svg?style=flat&logo=jupyter&logoColor=F37726&labelColor=31393F)
![Docker](https://img.shields.io/badge/Docker-329DEE?style=flat&logo=docker&logoColor=white&labelColor=329DEE)

![License](https://img.shields.io/badge/license-CC--BY--SA--4.0-31393F?style=flat&logo=creativecommons&logoColor=black&labelColor=white)

This module explores using PySpark to stream and analyze NYC Taxi trip data for Green Taxi Trips October 2019


## Tech Stack
- [PySpark](https://spark.apache.org/docs/latest/api/python/user_guide)
- [Docker](https://docs.docker.com/get-docker/)
- [Pipenv](https://pipenv.pypa.io/en/latest/)


## Steps to run

**1.** Clone the repository:
```shell
git clone https://github.com/sl2902/de-zoomcamp-2.git
```

**2.** Change the working directory
```shell
cd 06_streaming/
```

**3.** Enable the virtual environment:
```shell
pipenv shell
```

**4.** Run docker compose:
```shell
docker compose up -d
```

4.1 Enter redpand-1 container:
```shell
docker exec -it redpanda-1 bash
```

4.2 Create green-trips topic:
```shell
rpk topic create green-trips
```

4.3 To delete a topic:
```shell
rpk topic delete green-trips
```

4.4 To consume a topic - to list 5 records:
```shell
rpk topic consume green-trips -n 5
```

4.5 Exit the container
```shell
exit
```

**5.** Open two terminals. On one terminal, start the consumer:
```shell
python green_taxi_trips_consumer.py
```

5.1 In the other terminal, start the producer:
```shell
python green_taxi_trips_producer.py
```
