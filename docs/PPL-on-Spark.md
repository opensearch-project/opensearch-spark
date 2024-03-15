# Running PPL On Spark Reference Manual

## Overview

This module provides the support for running [PPL](https://github.com/opensearch-project/piped-processing-language) queries on Spark using direct logical plan
translation between PPL's logical plan to Spark's Catalyst logical plan.

### What is PPL ?
OpenSearch PPL, or Pipe Processing Language, is a query language used with the OpenSearch platform and now Apache Spark.
PPL allows users to retrieve, query, and analyze data by using commands that are piped together, making it easier to understand and compose complex queries.
Its syntax is inspired by Unix pipes, which enables chaining of commands to transform and process data. 
With PPL, users can filter, aggregate, and visualize data in multiple datasources in a more intuitive manner compared to traditional query languages

### Context

The next concepts are the main purpose of introduction this functionality:
- Transforming PPL to become OpenSearch default query language (specifically for logs/traces/metrics signals)
- Promoting PPL as a viable candidate for the proposed  CNCF Observability universal query language.
- Seamlessly Interact with different datasources such as S3 / Prometheus / data-lake leveraging spark execution.
- Using spark's federative capabilities as a general purpose query engine to facilitate complex queries including joins 
- Improve and promote PPL to become extensible and general purpose query language to be adopted by the community


### Running PPL Commands:

In order to run PPL commands, you will need to perform the following tasks:

#### PPL Build & Run

To build and run this PPL in Spark, you can run:

```
sbt clean sparkPPLCosmetic/publishM2
```
then add org.opensearch:opensearch-spark_2.12 when run spark application, for example,
```
bin/spark-shell --packages "org.opensearch:opensearch-spark-ppl_2.12:0.3.0-SNAPSHOT"
```

### PPL Extension Usage

To use PPL to Spark translation, you can run Spark with PPL extension:

```
spark-sql --conf "spark.sql.extensions=org.opensearch.flint.spark.FlintPPLSparkExtensions"
```

### Running With both Flint & PPL Extensions
In order to make use of both flint and ppl extension, one can simply add both jars (`org.opensearch:opensearch-spark-ppl_2.12:0.3.0-SNAPSHOT`,`org.opensearch:opensearch-spark_2.12:0.3.0-SNAPSHOT`) to the cluster's
classpath.

Next need to configure both extensions :
```
spark-sql --conf "spark.sql.extensions='org.opensearch.flint.spark.FlintPPLSparkExtensions, org.opensearch.flint.spark.FlintSparkExtensions'"
```

Once this is done, spark will allow both extensions to parse the query (SQL / PPL) and allow the correct execution of the query.
In addition, PPL queries will enjoy the acceleration capabilities supported by the Flint plugins as described [here](index.md)

