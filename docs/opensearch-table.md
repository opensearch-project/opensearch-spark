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

## Data Types
The following table defines the data type mapping between OpenSearch index field type and Spark data type.

| **OpenSearch FieldType** | **SparkDataType**                 |
|--------------------------|-----------------------------------|
| boolean                  | BooleanType                       |
| long                     | LongType                          |
| integer                  | IntegerType                       |
| short                    | ShortType                         |
| byte                     | ByteType                          |
| double                   | DoubleType                        |
| float                    | FloatType                         |
| half_float               | FloatType                         |
| date(Timestamp)          | TimestampType                     |
| date(Date)               | DateType                          |
| keyword                  | StringType, VarcharType, CharType |
| text                     | StringType(meta(osType)=text)     |
| object                   | StructType                        |
| alias                    | Inherits referenced field type    |
| ip                       | IPAddress(UDT)                    |

* OpenSearch data type date is mapped to Spark data type based on the format:
    * Map to DateType if format = strict_date, (we also support format = date, may change in future)
    * Map to TimestampType if format = strict_date_optional_time_nanos, (we also support format =
      strict_date_optional_time | epoch_millis, may change in future)
* Spark data types VarcharType(length) and CharType(length) are both currently mapped to OpenSearch 
  data type *keyword*, dropping their length property. On the other hand, OpenSearch data type 
  *keyword* only maps to StringType.
* Spark data type MapType is mapped to an empty OpenSearch object. The inner fields then rely on
  dynamic mapping. On the other hand, OpenSearch data type *object* only maps to StructType.
* Spark data type DecimalType is mapped to an OpenSearch double. On the other hand, OpenSearch data 
  type *double* only maps to DoubleType.
* OpenSearch alias fields allow alternative names for existing fields in the schema without duplicating data. They inherit the data type and nullability of the referenced field and resolve dynamically to the primary field in queries.
* OpenSearch multi-fields on text field is supported. These multi-fields are stored as part of the field's metadata and cannot be directly selected. Instead, they are automatically utilized during the DSL query translation process.
* IPAddress type cannot be directly compared with string type literal/field (e.g. '192.168.10.10'). Use `ip_equal` function like `ip_equal(ip, '192.168.0.10')`

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
