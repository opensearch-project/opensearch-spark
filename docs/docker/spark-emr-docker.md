# Running Queries with Spark EMR in Docker

Spark EMR images are available on the Amazon ECR Public Gallery. These can be modified to
be able to include the OpenSearch Spark PPL extension. With the OpenSearch Spark PPL
extension, the docker image can be used to test PPL commands.

The Spark EMR image will run an Apache Spark app if one was specified and then shutdown.

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

There are sample files in this repository at `docker/spark-emr-sample` They can be used to
run the Spark EMR container:

```
docker compose up
```

Remove the docker resources afterwards with:

```
docker compose down
```

### Configuration

There is a file `docker/spark-emr-sample/.env` that can be edited to change some settings.

| Variable Name  | Description                                       |
|----------------|---------------------------------------------------|
| PPL_JAR        | Path to the PPL Jar file                          |

## Logs

The logs are available in `/var/log/spark` in the docker container.

STDERR for the app run is available in `/var/log/spark/user/stderr`.

STDOUT for the app
run is available in `/var/log/spark/user/stdout`.

## Manual Setup

Need to create two directories. These directories will be bound to the directories in the
image.

Look in `docker/spark-emr-sample` in this repository for samples of the directories
described below.

### logging-conf
Contains two shell scripts that are run during startup to configure logging.
* `run-adot-collector.sh`
* `run-fluentd-spark.sh`

Unless you need to make changes to the logging in the docker image, these can both be
empty shell scripts.

### spark-conf

Contains the Apache Spark configuration. Need to add three lines to the `spark-defaults.conf`
file:
```
spark.sql.legacy.createHiveTableByDefault false
spark.sql.extensions org.opensearch.flint.spark.FlintPPLSparkExtensions
spark.sql.catalog.dev org.apache.spark.opensearch.catalog.OpenSearchCatalog
```

## Create a Spark App

An Apache Spark app is needed to provide queries to be run on the Spark EMR instance.
The image has been tested with an app written in Scala.

An example app is available in this repository in `docker/spark-sample--app`.

### Bulid the Example App

The example app can be built using [SBT](https://www.scala-sbt.org/).
```
cd docker/spark-sample-app
sbt clean package
```

This will produce a Jar file in `docker/spark-sample-app/target/scala-2.12`
that can be used with the Spark EMR image.

## Prepare OpenSearch Spark PPL Extension

Create a local build or copy of the OpenSearch Spark PPL extension. Make a note of the
location of the Jar file as well as the name of the Jar file.

## Run the Spark EMR Image

The Spark EMR image can be run with the following command from the root of this repository:
```
docker run \
    --name spark-emr \
    -v ./docker/spark-emr-sample/logging-conf:/var/loggingConfiguration/spark \
    -v ./docker/spark-sample-app/target/scala-2.12:/app \
    -v ./docker/spark-emr-sample/spark-conf:/etc/spark/conf \
    -v <PATH_TO_SPARK_PPL_JAR_FILE>/<SPARK_PPL_JAR_FILE>:/usr/lib/spark/jars/<SPARK_PPL_JAR_FILE> \
    public.ecr.aws/emr-serverless/spark/emr-7.5.0:20241125 \
    driver \
    --class MyApp \
    /app/myapp_2.12-1.0.jar
```

* `--name spark-emr`
   Name the docker container `spark-emr`
* `-v ./docker/spark-emr-sample/logging-conf:/var/loggingConfiguration/spark`
   
   Bind the directory containing logging shell scripts to the docker image. Needs to bind
   to `/var/loggingConfiguration/spark` in the image.
* `-v ./docker/spark-sample-app/target/scala-2.12:/app`
   
   Bind the directory containing the Apache Spark app Jar file to a location in the
   docker image. The directory in the docker image must match the path used in the final
   argument.
* `-v ./docker/spark-emr-sample/spark-conf:/etc/spark/conf`

   Bind the directory containing the Apache Spark configuration. Needs to bind to
   `/etc/spark/conf` in the image.
* `<PATH_TO_SPARK_PPL_JAR_FILE>`
   Replace with the path to the directory containing the OpenSearch Spark PPL extension
   Jar file.
* `<SPARK_PPL_JAR_FILE>`
   Replace with the filename of the OpenSearch Spark PPL extension Jar file.
* `driver`
   Start the Spark EMR container as a driver. This will run `spark-submit` to run an
   app.
* `--class MyApp`
   The main class of the Spark App to run.
* `/app/myapp_2.12-1.0.jar`
   The full path within the docker container where the Jar file of the Spark app is
   located.
