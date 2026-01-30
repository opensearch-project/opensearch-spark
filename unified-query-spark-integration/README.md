# Unified Query Spark Integration

This module provides the integration layer between the Calcite-based unified query engine and Apache Spark. It enables consistent query behavior across OpenSearch and Spark by leveraging the unified query parser and transpiler.

## Overview

The unified-query-spark-integration module bridges the gap between OpenSearch's Calcite-based unified query engine and Spark's execution environment. It provides:

- **UnifiedQuerySparkParser**: A custom Spark SQL parser that routes queries through the Calcite engine for transpilation to Spark SQL
- **SparkSchema**: Implements Calcite's Schema interface by bridging Spark SQL catalogs and tables

## Architecture

```
┌─────────────────────────────────┬──────────────────────────────────────┬─────────────────────────────────┐
│ Spark SQL                       │ Unified Query Integration            │ Unified Query API               │
├─────────────────────────────────┼──────────────────────────────────────┼─────────────────────────────────┤
│ spark.sql("<query text>")       │                                      │                                 │
│            │                    │                                      │                                 │
│            ├───────────────────►│ UnifiedQuerySparkParser              │                                 │
│            │                    │           │                          │                                 │
│            │                    │           ├────── query text ───────►│   UnifiedQueryPlanner           │
│            │                    │           │                          │           │                     │
│            │                    │           │                          │           ▼                     │
│            │                    │           │                          │     Calcite RelNode             │
│            │                    │           │                          │           │                     │
│            │                    │           │                          │           ▼                     │
│            │                    │           │                          │  UnifiedQueryTranspiler         │
│            │                    │           │                          │           │                     │
│            │◄──────────────────────────────────── Spark SQL text ──────┘───────────┘                     │
│            ▼                    │                                      │                                 │
│ Spark built-in SQL parser       │                                      │                                 │
│            │                    │                                      │                                 │
│            ▼                    │                                      │                                 │
│      LogicalPlan                │                                      │                                 │
│            │                    │                                      │                                 │
│            ▼                    │                                      │                                 │
│        Execute                  │                                      │                                 │
└─────────────────────────────────┴──────────────────────────────────────┴─────────────────────────────────┘
```

When a query is submitted:
1. `UnifiedQuerySparkParser.parsePlan()` receives the query
2. `UnifiedQueryPlanner` parses the query and creates a Calcite RelNode (unified logical plan)
3. `UnifiedQueryTranspiler` converts the RelNode to Spark SQL using `OpenSearchSparkSqlDialect`
4. The generated SQL is passed to Spark's native parser for execution
5. If parsing fails (unsupported query), it falls back to the underlying Spark parser

#### Type Mapping

The following table shows how Spark SQL types are mapped to Unified Query types for query processing:

| Spark SQL Type | Unified Query Type | Notes |
|----------------|--------------|-------|
| `BooleanType` | `BOOLEAN` | |
| `ByteType` | `TINYINT` | |
| `ShortType` | `SMALLINT` | |
| `IntegerType` | `INTEGER` | |
| `LongType` | `BIGINT` | |
| `FloatType` | `REAL` | 4-byte single-precision |
| `DoubleType` | `DOUBLE` | |
| `DecimalType(p, s)` | `DECIMAL(p, s)` | Preserves precision and scale |
| `StringType` | `VARCHAR` | |
| `VarcharType(n)` | `VARCHAR(n)` | Preserves length |
| `CharType(n)` | `CHAR(n)` | Preserves length |
| `BinaryType` | `VARBINARY` | |
| `DateType` | `DATE` | |
| `TimestampType` | `TIMESTAMP_WITH_LOCAL_TIME_ZONE` | Timestamp with timezone |
| `TimestampNTZType` | `TIMESTAMP` | Timestamp without timezone |
| `ArrayType(T, nullable)` | `T ARRAY` | Preserves element nullability |
| `MapType(K, V, nullable)` | `(K, V) MAP` | Preserves value nullability |
| `StructType` | `RecordType` | Preserves field names and nullability |
| `NullType` | `NULL` | |
| `DayTimeIntervalType` | `INTERVAL_DAY*` | Maps to appropriate day-time interval |
| `YearMonthIntervalType` | `INTERVAL_YEAR*` | Maps to appropriate year-month interval |
| `UserDefinedType` | Delegates to `sqlType` | Unwraps to underlying SQL type |
| Unsupported types | `ANY` | Fallback for CalendarIntervalType, ObjectType, etc. |

## Dependencies

This module depends on:
- `unified-query-api`: OpenSearch's Calcite-based unified query engine (from OpenSearch SQL project)
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

- [PPL on Spark Architecture](../docs/ppl-lang/PPL-on-Spark.md)

## GitHub Reference

- [opensearch-spark#1136](https://github.com/opensearch-project/opensearch-spark/issues/1136) - Unified Query Spark Integration

## License

See the [LICENSE](../LICENSE) file for our project's licensing.

## Copyright

Copyright OpenSearch Contributors. See [NOTICE](../NOTICE) for details.
