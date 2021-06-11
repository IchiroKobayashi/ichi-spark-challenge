# Usage

## Prerequisites
Make sure you have installed all of the following prerequisites on your development machine

- Git [- Download Git](https://git-scm.com/downloads)
- Docker [- Get Docker](https://docs.docker.com/get-docker/)

## Versions
- Python 3.9
- Apache Spark 3.0.1
- Apache Hadoop 3.2

## Build a local environment
Start docker container
```
cd [WORK_DIRECTORY]/ichi-spark-challenge
docker-compose up -d --remove-orphans
```

## To run Apache Spark at Once
Start history server and worker server
```
sh ./scripts/start.sh
```

## To run Apache Spark Individually
Start each history server and worker server individually
```
# history server
docker exec -it spark-master bash
/spark/sbin/start-history-server.sh
```
```
# Worker Node 1
docker exec -it spark-worker-1 bash
/spark/sbin/start-slave.sh spark://spark-master:7077
```
```
# Worker Node 2
docker exec -it spark-worker-2 bash
/spark/sbin/start-slave.sh spark://spark-master:7077
```

## To access local WebUI (check the details of cluster & master node)
```
http://localhost:8080/
```

## To execute spark application
Execute spark application
```
docker exec -it spark-master bash
/spark/bin/spark-submit /app/main.py
```