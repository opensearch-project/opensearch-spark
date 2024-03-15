# OpenSearch Flint

OpenSearch Flint is ... It consists of two modules:

- `flint-core`: a module that contains Flint specification and client.
- `flint-spark-integration`: a module that provides Spark integration for Flint and derived dataset based on it.
- `ppl-spark-integration`: a module that provides PPL query execution on top of Spark See [PPL repository](https://github.com/opensearch-project/piped-processing-language).

## Documentation

Please refer to the [Flint Index Reference Manual](./docs/index.md) for more information.
For PPL language see [PPL Reference Manual](https://github.com/opensearch-project/sql/blob/main/docs/user/ppl/index.rst) for more information.

## Prerequisites

Version compatibility:

| Flint version | JDK version | Spark version | Scala version | OpenSearch |
|---------------|-------------|---------------|---------------|------------|
| 0.1.0         | 11+         | 3.3.1         | 2.12.14       | 2.6+       |
| 0.2.0         | 11+         | 3.3.1         | 2.12.14       | 2.6+       |
| 0.3.0         | 11+         | 3.3.2         | 2.12.14       | 2.6+       |

## Flint Extension Usage 

To use this application, you can run Spark with Flint extension:

```
spark-sql --conf "spark.sql.extensions=org.opensearch.flint.spark.FlintSparkExtensions"
```

## PPL Extension Usage

To use PPL to Spark translation, you can run Spark with PPL extension:

```
spark-sql --conf "spark.sql.extensions=org.opensearch.flint.spark.FlintPPLSparkExtensions"
```

### Running With both Extension 
```
spark-sql --conf "spark.sql.extensions='org.opensearch.flint.spark.FlintPPLSparkExtensions, org.opensearch.flint.spark.FlintSparkExtensions'"
```

## Build

To build and run this application with Spark, you can run:

```
sbt clean standaloneCosmetic/publishM2
```
then add org.opensearch:opensearch-spark_2.12 when run spark application, for example,
```
bin/spark-shell --packages "org.opensearch:opensearch-spark_2.12:0.3.0-SNAPSHOT"
```

### PPL Build & Run 

To build and run this PPL in Spark, you can run:

```
sbt clean sparkPPLCosmetic/publishM2
```
then add org.opensearch:opensearch-spark_2.12 when run spark application, for example,
```
bin/spark-shell --packages "org.opensearch:opensearch-spark-ppl_2.12:0.3.0-SNAPSHOT"
```

## Code of Conduct

This project has adopted an [Open Source Code of Conduct](./CODE_OF_CONDUCT.md).

## Security

If you discover a potential security issue in this project we ask that you notify AWS/Amazon Security via our [vulnerability reporting page](http://aws.amazon.com/security/vulnerability-reporting/). Please do **not** create a public GitHub issue.

## License

See the [LICENSE](./LICENSE.txt) file for our project's licensing. We will ask you to confirm the licensing of your contribution.

## Copyright

Copyright OpenSearch Contributors. See [NOTICE](./NOTICE) for details.
