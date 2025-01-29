# End-to-End Tests

## Overview

The end-to-end tests start the integration test docker cluster and execute queries against it. Queries can be
sent to the Spark master or to OpenSearch server (using async query).

The tests will run a query and compare the results to an expected results file.

There are four types of tests:
1. SQL queries sent to the Spark master
2. PPL queries sent to the Spark master
3. SQL queries sent to the OpenSearch server as an async query
4. PPL queries sent to the OpenSearch server as an async query

## Running the End-to-End Tests

The tests can be run using SBT:

```shell
sbt e2etest/test
```

## Test Structure

### SQL Queries for Spark Master

Create two files:
* `e2e-test/src/test/resources/spark/queries/sql/[NAME].sql`
* `e2e-test/src/test/resources/spark/queries/sql/[NAME].results`

The `*.sql` file contains only the SQL query on one line.

The `*.results` file contains the results in CSV format with a header (column names).

### PPL Queries for Spark Master

Create two files:
* `e2e-test/src/test/resources/spark/queries/ppl/[NAME].ppl`
* `e2e-test/src/test/resources/spark/queries/ppl/[NAME].results`

The `*.ppl` file contains only the PPL query on one line.

The `*.results` file contains the results in CSV format with a header (column names).

### SQL Queries for OpenSearch Async API

Create two files:
* `e2e-test/src/test/resources/opensearch/queries/sql/[NAME].sql`
* `e2e-test/src/test/resources/opensearch/queries/sql/[NAME].results`

The `*.sql` file contains only the SQL query on one line.

The `*.results` file contains the results in JSON format. The format is the exact output from the REST call
to get the async query results (`_plugins/_async_query/[QUERY_ID]`).

Results example:
```json
{
  "status": "SUCCESS",
  "schema": [
    {
      "name": "id",
      "type": "integer"
    },
    {
      "name": "name",
      "type": "string"
    }
  ],
  "datarows": [
    [
      1,
      "Foo"
    ],
    [
      2,
      "Bar"
    ]
  ],
  "total": 2,
  "size": 2
}
```

### PPL Queries for OpenSearch Async API

Create two files:
* `e2e-test/src/test/resources/opensearch/queries/ppl/[NAME].ppl`
* `e2e-test/src/test/resources/opensearch/queries/ppl/[NAME].results`

The `*.ppl` file contains only the PPL query on one line.

The `*.results` file contains the results in JSON format. The format is the exact output from the REST call
to get the async query results (`_plugins/_async_query/[QUERY_ID]`).

Results example:
```json
{
  "status": "SUCCESS",
  "schema": [
    {
      "name": "id",
      "type": "integer"
    },
    {
      "name": "name",
      "type": "string"
    }
  ],
  "datarows": [
    [
      1,
      "Foo"
    ],
    [
      2,
      "Bar"
    ]
  ],
  "total": 2,
  "size": 2
}
```