# Unified Query Spark Integration

This module provides the integration layer between the [unified-query-api](https://github.com/opensearch-project/sql) Calcite-based PPL engine and Apache Spark. It enables consistent PPL (Piped Processing Language) query behavior across OpenSearch and Spark by leveraging the Calcite-based PPL parser and transpiler.

## Overview

The unified-query-spark-integration module bridges the gap between OpenSearch's Calcite-based PPL engine and Spark's execution environment. It provides:

- **SparkSchema Adapter**: Maps Spark's catalog system to Calcite's schema representation
- **UnifiedQuerySparkParser**: Routes PPL queries through the Calcite engine for transpilation to Spark SQL
- **Function Wrappers**: Adapts PPL functions (scalar and aggregate) for execution in Spark
- **Type Converters**: Bidirectional conversion between Calcite and Spark type systems

## Architecture

```
PPL Query → UnifiedQuerySparkParser → Calcite PPL Parser → Calcite RelNode 
         → OpenSearchSparkSqlDialect → Spark SQL → Spark Catalyst → Execution
```

When enabled via configuration, PPL queries are:
1. Parsed by the Calcite-based PPL parser from unified-query-api
2. Converted to a Calcite RelNode (logical plan)
3. Transpiled to Spark SQL using OpenSearchSparkSqlDialect
4. Executed by Spark's native SQL engine

## Configuration

Enable the Calcite-based PPL engine with:

```
spark.flint.ppl.calcite.enabled=true
```

When disabled (default), the legacy Spark PPL parser is used for backward compatibility.

## Dependencies

This module depends on:
- `flint-core`: Core Flint functionality
- `flint-commons`: Common utilities
- `unified-query-api`: OpenSearch's Calcite-based PPL engine

## Usage

### With Spark Session

```scala
val spark = SparkSession.builder()
  .appName("PPL with Calcite")
  .config("spark.sql.extensions", "org.opensearch.flint.spark.FlintPPLSparkExtensions")
  .config("spark.flint.ppl.calcite.enabled", "true")
  .getOrCreate()

// Execute PPL queries - they will be transpiled via Calcite
spark.sql("source = myTable | where age > 25 | fields name, age")
```

### Programmatic Transpilation

```scala
import org.opensearch.flint.spark.query.UnifiedQuerySparkParser

val parser = new UnifiedQuerySparkParser(spark, useCalcite = true)
val sparkSql = parser.transpileToSparkSql("source = myTable | where age > 25")
// Returns: "SELECT * FROM myTable WHERE age > 25"
```

## Build

Build the module with:

```bash
sbt unifiedQuerySparkIntegration/compile
```

Run tests with:

```bash
sbt unifiedQuerySparkIntegration/test
```

## Related Documentation

- [PPL Language Reference](../docs/ppl-lang/README.md)
- [PPL on Spark Architecture](../docs/ppl-lang/PPL-on-Spark.md)
- [Design Document](../.kiro/specs/ppl-calcite-spark-unification/design.md)

## GitHub Reference

- [opensearch-spark#1203](https://github.com/opensearch-project/opensearch-spark/issues/1203) - PPL-Calcite Spark Unification

## License

See the [LICENSE](../LICENSE) file for our project's licensing.

## Copyright

Copyright OpenSearch Contributors. See [NOTICE](../NOTICE) for details.
