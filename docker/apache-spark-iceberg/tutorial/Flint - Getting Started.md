## Flint - Getting Started
This tutorial introduces the usage of Flint as a caching and acceleration platform on top of spark.
The content of the tutorial is based on [Iceberg spark Github Example](https://github.com/databricks/docker-spark-iceberg) and tutorial and expands it with Flint's capabilities and functionality.



### Catalog
In Spark, a catalog is a repository for organizing and accessing datasets, such as tables and indexes, within Spark applications. Catalogs allow users to logically separate and manage datasets from various sources.


In this initial setup we created 2 catalogs :
 - Iceberg - for Iceberg table format demonstration
 - Demo - for OpenSearch index acceleration demonstration

````yaml
spark.sql.extensions                   org.opensearch.flint.spark.FlintPPLSparkExtensions, org.opensearch.flint.spark.FlintSparkExtensions, org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
spark.sql.catalog.demo                 org.apache.spark.opensearch.catalog.OpenSearchCatalog

spark.sql.catalog.iceberg              org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.iceberg.type         rest
spark.sql.catalog.iceberg.uri          http://rest:8181
spark.sql.catalog.iceberg.io-impl      org.apache.iceberg.aws.s3.S3FileIO
spark.sql.catalog.iceberg.warehouse    s3://warehouse/wh/
spark.sql.catalog.iceberg.s3.endpoint  http://minio:9000

spark.sql.defaultCatalog               iceberg
````
Here are the [spark-defaults.conf](../spark-defaults.conf)

### SQL Console
Now we can see the default catalog
```sql
% SHOW CATALOGS
catalog
-------
demo
spark_catalog

```

#### Databased & Tables in Catalog
Each catalog contains databases where the default one us called `default`

```sql
% SHOW DATABASES IN iceberg;
-------
namespace
default
```

Each `database` contains tables :
```sql
% SHOW TABLES IN iceberg.default;
namespace	tableName	isTemporary
-------     --------    -----------
default	   iceberg_table	False
```

Here we see an `iceberg_table` that was created previously.

### Creating the NYC database with its taxis table
```sql
CREATE TABLE IF NOT EXISTS iceberg.nyc.taxis (
    VendorID              bigint,
    tpep_pickup_datetime  timestamp,
    tpep_dropoff_datetime timestamp,
    passenger_count       double,
    trip_distance         double,
    RatecodeID            double,
    store_and_fwd_flag    string,
    PULocationID          bigint,
    DOLocationID          bigint,
    payment_type          bigint,
    fare_amount           double,
    extra                 double,
    mta_tax               double,
    tip_amount            double,
    tolls_amount          double,
    improvement_surcharge double,
    total_amount          double,
    congestion_surcharge  double,
    airport_fee           double
)
USING iceberg
PARTITIONED BY (days(tpep_pickup_datetime))
```

### Populating the NYC.Taxis table using the dataset

The following code would populate the table with the ...

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Jupyter").getOrCreate()

for filename in [
    "yellow_tripdata_2022-04.parquet",
    "yellow_tripdata_2022-03.parquet",
    "yellow_tripdata_2022-02.parquet",
    "yellow_tripdata_2022-01.parquet",
    "yellow_tripdata_2021-12.parquet",
]:
    df = spark.read.parquet(f"/home/iceberg/data/{filename}")
    df.write.mode("append").saveAsTable("nyc.taxis")
```

### Querying the Data

Using SQL ...

```sql
select * from iceberg.nyc.taxis limit 10;
----------------------------------------------------------------------------------------------------------------------------------------
VendorID	tpep_pickup_datetime	tpep_dropoff_datetime	passenger_count	trip_distance	RatecodeID	store_and_fwd_flag	PULocationID	DOLocationID	payment_type	fare_amount	extra	mta_tax	tip_amount	tolls_amount	improvement_surcharge	total_amount	congestion_surcharge	airport_fee
2	2022-02-25 08:12:28	2022-02-25 09:02:31	1.0	12.46	1.0	N	138	186	1	42.5	0.5	0.5	10.82	6.55	0.3	64.92	2.5	1.25
2	2022-02-25 09:04:15	2022-02-25 09:09:12	1.0	0.71	1.0	N	186	90	2	5.0	0.5	0.5	0.0	0.0	0.3	8.8	2.5	0.0
2	2022-02-25 09:14:20	2022-02-25 09:22:25	1.0	1.17	1.0	N	234	161	1	7.0	0.5	0.5	2.7	0.0	0.3	13.5	2.5	0.0
2	2022-02-25 10:13:47	2022-02-25 10:52:18	1.0	9.71	1.0	N	138	68	1	35.5	0.5	0.5	9.42	6.55	0.3	56.52	2.5	1.25
2	2022-02-25 10:59:43	2022-02-25 11:14:48	1.0	1.58	1.0	N	100	163	1	10.5	0.5	0.5	4.29	0.0	0.3	18.59	2.5	0.0
2	2022-02-25 12:26:44	2022-02-25 12:50:31	1.0	10.81	1.0	N	138	230	1	31.0	0.5	0.5	8.52	6.55	0.3	51.12	2.5	1.25
2	2022-02-25 12:51:53	2022-02-25 12:59:59	1.0	0.82	1.0	N	230	237	1	6.5	0.5	0.5	2.06	0.0	0.3	12.36	2.5	0.0
2	2022-02-25 13:03:07	2022-02-25 13:14:07	1.0	1.87	1.0	N	237	50	1	9.0	0.5	0.5	3.2	0.0	0.3	16.0	2.5	0.0
2	2022-02-25 16:02:40	2022-02-25 16:36:45	1.0	10.47	1.0	N	138	163	1	33.0	0.0	0.5	8.82	6.55	0.3	52.92	2.5	1.25
2	2022-02-25 16:43:33	2022-02-25 16:53:51	1.0	1.95	1.0	N	162	107	1	9.0	0.0	0.5	2.46	0.0	0.3	14.76	2.5	0.0
```
Using PPL ...

```sql
source=`nyc`.`taxis` | head 10;
----------------------------------------------------------------------------------------------------------------------------------------
VendorID	tpep_pickup_datetime	tpep_dropoff_datetime	passenger_count	trip_distance	RatecodeID	store_and_fwd_flag	PULocationID	DOLocationID	payment_type	fare_amount	extra	mta_tax	tip_amount	tolls_amount	improvement_surcharge	total_amount	congestion_surcharge	airport_fee
2	2022-02-25 08:12:28	2022-02-25 09:02:31	1.0	12.46	1.0	N	138	186	1	42.5	0.5	0.5	10.82	6.55	0.3	64.92	2.5	1.25
2	2022-02-25 09:04:15	2022-02-25 09:09:12	1.0	0.71	1.0	N	186	90	2	5.0	0.5	0.5	0.0	0.0	0.3	8.8	2.5	0.0
2	2022-02-25 09:14:20	2022-02-25 09:22:25	1.0	1.17	1.0	N	234	161	1	7.0	0.5	0.5	2.7	0.0	0.3	13.5	2.5	0.0
2	2022-02-25 10:13:47	2022-02-25 10:52:18	1.0	9.71	1.0	N	138	68	1	35.5	0.5	0.5	9.42	6.55	0.3	56.52	2.5	1.25
2	2022-02-25 10:59:43	2022-02-25 11:14:48	1.0	1.58	1.0	N	100	163	1	10.5	0.5	0.5	4.29	0.0	0.3	18.59	2.5	0.0
2	2022-02-25 12:26:44	2022-02-25 12:50:31	1.0	10.81	1.0	N	138	230	1	31.0	0.5	0.5	8.52	6.55	0.3	51.12	2.5	1.25
2	2022-02-25 12:51:53	2022-02-25 12:59:59	1.0	0.82	1.0	N	230	237	1	6.5	0.5	0.5	2.06	0.0	0.3	12.36	2.5	0.0
2	2022-02-25 13:03:07	2022-02-25 13:14:07	1.0	1.87	1.0	N	237	50	1	9.0	0.5	0.5	3.2	0.0	0.3	16.0	2.5	0.0
2	2022-02-25 16:02:40	2022-02-25 16:36:45	1.0	10.47	1.0	N	138	163	1	33.0	0.0	0.5	8.82	6.55	0.3	52.92	2.5	1.25
2	2022-02-25 16:43:33	2022-02-25 16:53:51	1.0	1.95	1.0	N	162	107	1	9.0	0.0	0.5	2.46	0.0	0.3	14.76	2.5	0.0

```

### Creating a MV using OpenSearch Index Acceleration

This example shows how to create ...

```sql
 CREATE MATERIALIZED VIEW nyc_taxi_mv
 AS select * from iceberg.nyc.taxis limit 10
 WITH (
   auto_refresh = false
 )
 
 ```