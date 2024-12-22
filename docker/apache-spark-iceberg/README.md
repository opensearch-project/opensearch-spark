# Sanity Test OpenSearch Spark PPL
This document shows how to locally test OpenSearch PPL commands on top of Spark using docker-compose. 

See instructions for running docker-compose [here](../../docs/spark-docker.md)

Once the docker services are running,[connect to the spark-sql](../../docs/local-spark-ppl-test-instruction.md#running-spark-shell)

In the spark-sql shell - [run the next create table statements](../../docs/local-spark-ppl-test-instruction.md#testing-ppl-commands)

Now PPL commands can [run](../../docs/local-spark-ppl-test-instruction.md#test-grok--top-commands-combination) on top of the table just created

### Using Iceberg Tables
The following example utilize https://iceberg.apache.org/ table as an example
```sql
CREATE TABLE iceberg_table (
  id INT,
  name STRING,
  age INT,
  city STRING
)
USING iceberg
PARTITIONED BY (city)
LOCATION 'file:/tmp/iceberg-tables/default/iceberg_table';

INSERT INTO iceberg_table VALUES
                              (1, 'Alice', 30, 'New York'),
                              (2, 'Bob', 25, 'San Francisco'),
                              (3, 'Charlie', 35, 'New York'),
                              (4, 'David', 40, 'Chicago'),
                              (5, 'Eve', 28, 'San Francisco');
```

### PPL queries 
```sql
 source=`default`.`iceberg_table`;
 source=`default`.`iceberg_table` | where age > 30 | fields id, name, age, city | sort - age;
 source=`default`.`iceberg_table` | where age > 30 | stats count() by city;
 source=`default`.`iceberg_table` | stats avg(age) by city;
```