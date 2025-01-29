# Running Queries with Apache Spark in Docker

There are [Bitnami Apache Spark docker images](https://hub.docker.com/r/bitnami/spark).
These can be modified to be able to include the OpenSearch Spark PPL extension. With the OpenSearch
Spark PPL extension, the docker image can be used to test PPL commands.

The Bitnami Apache Spark image can be used to run a Spark cluster and also to run
`spark-shell` for running queries.

## Prepare OpenSearch Spark PPL Extension

Create a local build or copy of the OpenSearch Spark PPL extension. Make a note of the
location of the Jar file as well as the name of the Jar file.

From the root of this repository, build the OpenSearch Spark PPL extension with:

```
sbt clean
sbt assembly
```

Refer to the [Developer Guide](../../DEVELOPER_GUIDE.md) for more information.

## Using Docker Compose

There are sample files in this repository at `docker/apache-spark-sample` They can be used to
start up both nodes with the command:

```
docker compose up -d
```

The cluster can be stopped with:

```
docker compose down
```

### Configuration

There is a file `docker/apache-spark-sample/.env` that can be edited to change some settings.

| Variable Name  | Description                                       |
|----------------|---------------------------------------------------|
| MASTER_UI_PORT | Host port to bind to port 8080 of the master node |
| MASTER_PORT    | Host port to bind to port 7077 of the master node |
| UI_PORT        | Host port to bind to port 4040 of the master node |
| PPL_JAR        | Path to the PPL Jar file                          |

## Running Spark Shell

Can run `spark-shell` on the master node.

```
docker exec -it apache-spark-sample-spark-1 /opt/bitnami/spark/bin/spark-shell
```

Within the Spark Shell, you can submit queries, including PPL queries. For example a sample
table can be created, populated and finally queried using PPL.

```
spark.sql("CREATE TABLE test_table(id int, name varchar(100))")
spark.sql("INSERT INTO test_table (id, name) VALUES(1, 'Foo')")
spark.sql("INSERT INTO test_table (id, name) VALUES(2, 'Bar')")
spark.sql("source=test_table | eval x = id + 5 | fields x, name").show()
```

For further information, see the [Spark PPL Test Instructions](../ppl-lang/local-spark-ppl-test-instruction.md)

## Manual Setup

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
