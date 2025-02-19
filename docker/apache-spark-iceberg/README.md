# Sanity Test OpenSearch Spark PPL
This document shows how to locally test OpenSearch PPL commands on top of Spark IceBerg using docker-compose based of [docker-spark-Iceberg](https://github.com/databricks/docker-spark-iceberg) opensource repository. 

Additional instructions for running the original test harness [here](https://iceberg.apache.org/spark-quickstart/)

## Running Docker Compose
For running docker-compose run: `docker compose up -d`

See additional instructions [here](../../docs/spark-docker.md)

## Running Spark Shell

Can run `spark-sql` on the master node using `docker exec`:

```
docker exec -it spark-iceberg /opt/spark/bin/spark-sql
```

In the spark-sql shell - [run the next create table statements](../../docs/local-spark-ppl-test-instruction.md#testing-ppl-commands)

Now PPL commands can [run](../../docs/local-spark-ppl-test-instruction.md#test-grok--top-commands-combination) on top of the table just created

### Using Iceberg Tables
The following example utilize [Icberg](https://iceberg.apache.org/) table as an example
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