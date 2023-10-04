# Spark SQL Application

We have two applications: SQLJob and FlintJob.

SQLJob is designed for EMR Spark, executing SQL queries and storing the results in the OpenSearch index in the following format:
```
"stepId":"<emr-step-id>",
"applicationId":"<spark-application-id>",
"schema": "json blob",
"result": "json blob"
```

FlintJob is designed for EMR Serverless Spark, executing SQL queries and storing the results in the OpenSearch index in the following format:

```
"jobRunId":"<emrs-job-id>",
"applicationId":"<spark-application-id>",
"schema": "json blob",
"result": "json blob",
"dataSourceName":"<opensearch-data-source-name>"
```

## Prerequisites

+ Spark 3.3.1
+ Scala 2.12.15
+ flint-spark-integration
+ ppl-spark-integration

## Usage

To use these applications, you can run Spark with Flint extension:

SQLJob
```
./bin/spark-submit \
    --class org.opensearch.sql.SQLJob \
    --jars <flint-spark-integration-jar> \
    sql-job.jar \
    <spark-extensions-list> \
    <spark-sql-query> \
    <opensearch-index> \
    <opensearch-host> \
    <opensearch-port> \
    <opensearch-scheme> \
    <opensearch-auth> \
    <opensearch-region> \
```

FlintJob
```
aws emr-serverless start-job-run \
    --region <region-name> \
    --application-id <application-id> \
    --execution-role-arn <execution-role>  \
    --job-driver '{"sparkSubmit": {"entryPoint": "<flint-job-s3-path>", \
      "entryPointArguments":["'<spark extensions>'","'<sql-query>'", "<result-index>", "<data-source-name>"], \
      "sparkSubmitParameters":"--class org.opensearch.sql.FlintJob \
        --conf spark.hadoop.fs.s3.customAWSCredentialsProvider=com.amazonaws.emr.AssumeRoleAWSCredentialsProvider \
        --conf spark.emr-serverless.driverEnv.ASSUME_ROLE_CREDENTIALS_ROLE_ARN=<role-to-access-s3-and-opensearch> \
        --conf spark.executorEnv.ASSUME_ROLE_CREDENTIALS_ROLE_ARN=<role-to-access-s3-and-opensearch> \
        --conf spark.hadoop.aws.catalog.credentials.provider.factory.class=com.amazonaws.glue.catalog.metastore.STSAssumeRoleSessionCredentialsProviderFactory \
        --conf spark.hive.metastore.glue.role.arn=<role-to-access-s3-and-opensearch> \
        --conf spark.jars=<path-to-AWSGlueDataCatalogHiveMetaStoreAuth-jar> \
        --conf spark.jars.packages=<flint-spark-integration-jar-name> \
        --conf spark.jars.repositories=<path-to-download_spark-integration-jar> \
        --conf spark.emr-serverless.driverEnv.JAVA_HOME=<java-home-in-emr-serverless-host> \
        --conf spark.executorEnv.JAVA_HOME=<java-home-in-emr-serverless-host> \
        --conf spark.datasource.flint.host=<opensearch-url> \
        --conf spark.datasource.flint.port=<opensearch-port> \
        --conf spark.datasource.flint.scheme=<http-or-https> \
        --conf spark.datasource.flint.auth=<auth-type> \
        --conf spark.datasource.flint.region=<region-name> \
        --conf spark.datasource.flint.customAWSCredentialsProvider=com.amazonaws.emr.AssumeRoleAWSCredentialsProvider \
        --conf spark.sql.extensions=org.opensearch.flint.spark.FlintSparkExtensions \
        --conf spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory "}}'
    <data-source-name>
```

## Result Specifications

Following example shows how the result is written to OpenSearch index after query execution.

Let's assume SQL query result is
```
+------+------+
|Letter|Number|
+------+------+
|A     |1     |
|B     |2     |
|C     |3     |
+------+------+
```
For SQLJob, OpenSearch index document will look like
```json
{
  "_index" : ".query_execution_result",
  "_id" : "A2WOsYgBMUoqCqlDJHrn",
  "_score" : 1.0,
  "_source" : {
    "result" : [
    "{'Letter':'A','Number':1}",
    "{'Letter':'B','Number':2}",
    "{'Letter':'C','Number':3}"
    ],
    "schema" : [
      "{'column_name':'Letter','data_type':'string'}",
      "{'column_name':'Number','data_type':'integer'}"
    ],
    "stepId" : "s-JZSB1139WIVU",
    "applicationId" : "application_1687726870985_0003"
  }
}
```

For FlintJob, OpenSearch index document will look like
```json
{
  "_index" : ".query_execution_result",
  "_id" : "A2WOsYgBMUoqCqlDJHrn",
  "_score" : 1.0,
  "_source" : {
    "result" : [
    "{'Letter':'A','Number':1}",
    "{'Letter':'B','Number':2}",
    "{'Letter':'C','Number':3}"
    ],
    "schema" : [
      "{'column_name':'Letter','data_type':'string'}",
      "{'column_name':'Number','data_type':'integer'}"
    ],
    "jobRunId" : "s-JZSB1139WIVU",
    "applicationId" : "application_1687726870985_0003",
    "dataSourceName": "myS3Glue",
    "status": "SUCCESS",
    "error": ""
  }
}
```

## Build

To build and run this application with Spark, you can run:

```
sbt clean sparkSqlApplicationCosmetic/publishM2
```

The jar file is located at `spark-sql-application/target/scala-2.12` folder.

## Test

To run tests, you can use:

```
sbt test
```

## Scalastyle

To check code with scalastyle, you can run:

```
sbt scalastyle
```

To check code with scalastyle, you can run:

```
sbt testScalastyle
```

## Code of Conduct

This project has adopted an [Open Source Code of Conduct](../CODE_OF_CONDUCT.md).

## Security

If you discover a potential security issue in this project we ask that you notify AWS/Amazon Security via our [vulnerability reporting page](http://aws.amazon.com/security/vulnerability-reporting/). Please do **not** create a public GitHub issue.

## License

See the [LICENSE](../LICENSE.txt) file for our project's licensing. We will ask you to confirm the licensing of your contribution.

## Copyright

Copyright OpenSearch Contributors. See [NOTICE](../NOTICE) for details.