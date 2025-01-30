# OpenSearch Table

## Overview

The `OpenSearchCatalog` class integrates Apache Spark with OpenSearch, allowing Spark to interact with OpenSearch indices as tables. This integration supports read and write operations, enabling seamless data processing and querying across Spark and OpenSearch.

## Configuration Parameters

To configure the `OpenSearchCatalog`, set the following parameters in your Spark session:

- **`opensearch.port`**: The port to connect to OpenSearch. Default is `9200`.
- **`opensearch.scheme`**: The scheme to use for the connection. Default is `http`. Valid values are `[http, https]`.
- **`opensearch.auth`**: The authentication method to use. Default is `noauth`. Valid values are `[noauth, sigv4, basic]`.
- **`opensearch.auth.username`**: The username for basic authentication.
- **`opensearch.auth.password`**: The password for basic authentication.
- **`opensearch.region`**: The AWS region to use for SigV4 authentication. Default is `us-west-2`. Used only when `auth` is `sigv4`.

## Usage

### Initializing the Catalog

To configure and initialize the catalog in your Spark session, set the following configurations:

```scala
spark.conf.set("spark.sql.catalog.dev", "org.apache.spark.opensearch.catalog.OpenSearchCatalog")
spark.conf.set("spark.sql.catalog.dev.opensearch.port", "9200")
spark.conf.set("spark.sql.catalog.dev.opensearch.scheme", "http")
spark.conf.set("spark.sql.catalog.dev.opensearch.auth", "noauth")
```

### Querying Data

Once the catalog is configured, you can use Spark SQL to query OpenSearch indices as tables:

- The namespace **MUST** be `default`.
- When using a wildcard index name, it **MUST** be wrapped in backticks.

Example:

```scala
val df = spark.sql("SELECT * FROM dev.default.my_index")
df.show()
```

Using a wildcard index name:
```scala
val df = spark.sql("SELECT * FROM dev.default.`my_index*`")
df.show()
```

Join two indices
```scala
val df = spark.sql("SELECT * FROM dev.default.my_index as t1 JOIN dev.default.my_index as t2 ON t1.id == t2.id")
df.show()
```

## Limitation
### catalog operation
- List Tables: Not supported.
- Create Table: Not supported.
- Alter Table: Not supported.
- Drop Table: Not supported.
- Rename Table: Not supported.

### table operation
- Table only support read operation, for instance, SELECT, DESCRIBE.

##  InputPartition
Each InputPartition represents a data split that can be processed by a single Spark task. The number of input partitions returned corresponds to the number of RDD partitions produced by this scan. An OpenSearch table can contain multiple indices, each comprising multiple shards. The input partition count is determined by multiplying the number of indices by the number of shards. Read requests are divided and executed in parallel across multiple shards on multiple nodes.
