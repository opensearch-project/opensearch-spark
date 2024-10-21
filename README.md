# OpenSearch Flint

OpenSearch consists of four modules:

- `flint-core`: a module that contains Flint specification and client.
- `flint-commons`: a module that provides a shared library of utilities and common functionalities, designed to easily extend Flint's capabilities.
- `flint-spark-integration`: a module that provides Spark integration for Flint and derived dataset based on it.
- `ppl-spark-integration`: a module that provides PPL query execution on top of Spark See [PPL repository](https://github.com/opensearch-project/piped-processing-language).

## Documentation

Please refer to the [Flint Index Reference Manual](./docs/index.md) for more information.

### PPL-Language

* For additional details on PPL commands, see [PPL Commands Docs](docs/ppl-lang/README.md)

* For additional details on Spark PPL Architecture, see [PPL Architecture](docs/ppl-lang/PPL-on-Spark.md)

* For additional details on Spark PPL commands project, see [PPL Project](https://github.com/orgs/opensearch-project/projects/214/views/2)

## Prerequisites

Version compatibility:

| Flint version | JDK version | Spark version | Scala version | OpenSearch |
|---------------|-------------|---------------|---------------|------------|
| 0.1.0         | 11+         | 3.3.1         | 2.12.14       | 2.6+       |
| 0.2.0         | 11+         | 3.3.1         | 2.12.14       | 2.6+       |
| 0.3.0         | 11+         | 3.3.2         | 2.12.14       | 2.13+      |
| 0.4.0         | 11+         | 3.3.2         | 2.12.14       | 2.13+      |
| 0.5.0         | 11+         | 3.5.1         | 2.12.14       | 2.17+      |
| 0.6.0         | 11+         | 3.5.1         | 2.12.14       | 2.17+      |

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
spark-sql --conf "spark.sql.extensions=org.opensearch.flint.spark.FlintPPLSparkExtensions,org.opensearch.flint.spark.FlintSparkExtensions"
```

## Build

To build and run this application with Spark, you can run (requires Java 11):

```
sbt clean standaloneCosmetic/publishM2
```
then add org.opensearch:opensearch-spark-standalone_2.12 when run spark application, for example,
```
bin/spark-shell --packages "org.opensearch:opensearch-spark-standalone_2.12:0.6.0-SNAPSHOT" \
                --conf "spark.sql.extensions=org.opensearch.flint.spark.FlintSparkExtensions" \
                --conf "spark.sql.catalog.dev=org.apache.spark.opensearch.catalog.OpenSearchCatalog"
```

### PPL Build & Run 

To build and run this PPL in Spark, you can run (requires Java 11):

```
sbt clean sparkPPLCosmetic/publishM2
```
then add org.opensearch:opensearch-spark-ppl_2.12 when run spark application, for example,
```
bin/spark-shell --packages "org.opensearch:opensearch-spark-ppl_2.12:0.6.0-SNAPSHOT" \
                --conf "spark.sql.extensions=org.opensearch.flint.spark.FlintPPLSparkExtensions" \
                --conf "spark.sql.catalog.dev=org.apache.spark.opensearch.catalog.OpenSearchCatalog"

```

## Code of Conduct

This project has adopted an [Open Source Code of Conduct](./CODE_OF_CONDUCT.md).

## Security

If you discover a potential security issue in this project we ask that you notify OpenSearch Security directly via email to security@opensearch.org. Please do **not** create a public GitHub issue.

## License

See the [LICENSE](./LICENSE.txt) file for our project's licensing. We will ask you to confirm the licensing of your contribution.

## Copyright

Copyright OpenSearch Contributors. See [NOTICE](./NOTICE) for details.
