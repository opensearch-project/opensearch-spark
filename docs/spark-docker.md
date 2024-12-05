# Running Queries with Apache Spark in Docker

There are [Bitnami Apache Spark docker images](https://hub.docker.com/r/bitnami/spark). These
can be modified to be able to include the OpenSearch Spark PPL extension. With the OpenSearch
Spark PPL extension, the docker image can be used to test PPL commands.

The Bitnami Apache Spark image can be used to run a Spark cluster and also to run
`spark-shell` for running queries.

## Setup

### spark-conf

Contains the Apache Spark configuration. Need to add three lines to the `spark-defaults.conf`
file:
```
spark.sql.legacy.createHiveTableByDefault false
spark.sql.extensions org.opensearch.flint.spark.FlintPPLSparkExtensions
spark.sql.catalog.dev org.apache.spark.opensearch.catalog.OpenSearchCatalog
```

An example file available in this repository at `docker/apache-spark-sample/spark-defaults.conf`

## Prepare OpenSearch Spark PPL Extension

Create a local build or copy of the OpenSearch Spark PPL extension. Make a note of the
location of the Jar file as well as the name of the Jar file.

## Run the Spark Cluster

Need to run a master node and a worker node. For these to communicate, first create a network
for them to use.

```
docker network create spark-network
```

### Master Node

The master node can be run with the following command:
```
docker run \
    -d \
    --name spark \
    --network spark-network \
    -p 8080:8080 \
    -p 7077:7077 \
    -p 4040:4040 \
    -e SPARK_MODE=master \
    -e SPARK_RPC_AUTHENTICATION_ENABLED=no \
    -e SPARK_RPC_ENCRYPTION_ENABLED=no \
    -e SPARK_LOCAL_STORAGE_ENCRYPTION_ENABLED=no \
    -e SPARK_SSL_ENABLED=no \
    -e SPARK_PUBLIC_DNS=localhost \
    -v <PATH_TO_SPARK_CONFIG_FILE>:/opt/bitnami/spark/conf/spark-defaults.conf \
    -v <PATH_TO_SPARK_PPL_JAR_FILE>/<SPARK_PPL_JAR_FILE>:/opt/bitnami/spark/jars/<SPARK_PPL_JAR_FILE> \
    bitnami/spark:3.5.3
```

* `-d`
   Run the container in the background and return to the shell
* `--name spark`
   Name the docker container `spark`
* `<PATH_TO_SPARK_CONFIG_FILE>`
   Replace with the path to the Spark configuration file.
* `<PATH_TO_SPARK_PPL_JAR_FILE>`
   Replace with the path to the directory containing the OpenSearch Spark PPL extension
   Jar file.
* `<SPARK_PPL_JAR_FILE>`
   Replace with the filename of the OpenSearch Spark PPL extension Jar file.

### Worker Node

The worker node can be run with the following command:
```
docker run \
    -d \
    --name spark-worker \
    --network spark-network \
    -e SPARK_MODE=worker \
    -e SPARK_MASTER_URL=spark://spark:7077 \
    -e SPARK_WORKER_MEMORY=1G \
    -e SPARK_WORKER_CORES=1 \
    -e SPARK_RPC_AUTHENTICATION_ENABLED=no \
    -e SPARK_RPC_ENCRYPTION_ENABLED=no \
    -e SPARK_LOCAL_STORAGE_ENCRYPTION_ENABLED=no \
    -e SPARK_SSL_ENABLED=no \
    -e SPARK_PUBLIC_DNS=localhost \
    -v <PATH_TO_SPARK_CONFIG_FILE>:/opt/bitnami/spark/conf/spark-defaults.conf \
    -v <PATH_TO_SPARK_PPL_JAR_FILE>/<SPARK_PPL_JAR_FILE>:/opt/bitnami/spark/jars/<SPARK_PPL_JAR_FILE> \
    bitnami/spark:3.5.3
```

* `-d`
  Run the container in the background and return to the shell
* `--name spark-worker`
  Name the docker container `spark-worker`
* `<PATH_TO_SPARK_CONFIG_FILE>`
  Replace with the path to the Spark configuration file.
* `<PATH_TO_SPARK_PPL_JAR_FILE>`
  Replace with the path to the directory containing the OpenSearch Spark PPL extension
  Jar file.
* `<SPARK_PPL_JAR_FILE>`
  Replace with the filename of the OpenSearch Spark PPL extension Jar file.

## Running Spark Shell

Can run `spark-shell` on the master node.

```
docker exec -it spark /opt/bitnami/spark/bin/spark-shell
```

Within the Spark Shell, you can submit queries, including PPL queries.

## Docker Compose Sample

There is a sample `docker-compose.yml` file in this repository at
`docker/apache-spark-sample/docker-compose.yml` It can be used to start up both nodes with
the command:

```
docker compose up -d
```

The cluster can be stopped with:

```
docker compose down
```