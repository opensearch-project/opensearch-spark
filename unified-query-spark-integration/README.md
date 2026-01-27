# Unified Query Spark Integration

This module provides the integration layer between the [unified-query-api](https://ci.opensearch.org/ci/dbc/snapshots/maven/) Calcite-based PPL engine and Apache Spark. It enables consistent PPL (Piped Processing Language) query behavior across OpenSearch and Spark by leveraging the Calcite-based PPL parser and transpiler.

## Overview

The unified-query-spark-integration module bridges the gap between OpenSearch's Calcite-based PPL engine and Spark's execution environment. It provides:

- **UnifiedQuerySparkParser**: A custom Spark SQL parser that routes PPL queries through the Calcite engine for transpilation to Spark SQL
- **SparkSchema**: Implements Calcite's Schema interface by bridging Spark SQL catalogs and tables

## Architecture

```
PPL Query → UnifiedQuerySparkParser → UnifiedQueryPlanner → Calcite RelNode 
         → UnifiedQueryTranspiler (OpenSearchSparkSqlDialect) → Spark SQL 
         → Spark Parser → LogicalPlan → Execution
```

When a PPL query is submitted:
1. `UnifiedQuerySparkParser.parsePlan()` receives the query
2. `UnifiedQueryPlanner` parses the PPL and creates a Calcite RelNode (logical plan)
3. `UnifiedQueryTranspiler` converts the RelNode to Spark SQL using `OpenSearchSparkSqlDialect`
4. The generated SQL is passed to Spark's native parser for execution
5. If parsing fails (non-PPL query), it falls back to the underlying Spark parser

## Key Components

### UnifiedQuerySparkParser

A custom `ParserInterface` implementation that:
- Intercepts query parsing in Spark
- Attempts PPL transpilation via the unified query planner
- Falls back to Spark's native parser for non-PPL queries (SQL, DDL, etc.)

### SparkSchema

Implements Calcite's `Schema` interface to expose Spark's catalog system:
- Maps Spark catalogs to Calcite schemas
- Provides lazy loading of databases and tables
- Converts Spark table schemas to Calcite row types

## Dependencies

This module depends on:
- `unified-query-api`: OpenSearch's Calcite-based PPL engine (from OpenSearch SQL project)
- Apache Spark SQL (provided)
- Apache Calcite (transitive via unified-query-api)

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

## GitHub Reference

- [opensearch-spark#1203](https://github.com/opensearch-project/opensearch-spark/issues/1203) - PPL-Calcite Spark Unification

## License

See the [LICENSE](../LICENSE) file for our project's licensing.

## Copyright

Copyright OpenSearch Contributors. See [NOTICE](../NOTICE) for details.
