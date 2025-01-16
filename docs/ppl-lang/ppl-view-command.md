## PPL `view` command

### Description
Using `view` command to materialize a query into a dedicated view:
In some cases it is required to construct a view (materialized into a view) of the query results.
This view can be later used as a source of continued queries for further slicing and dicing the data, in addition such tables can be also saved into a MV table that are pushed into OpenSearch and can be used for visualization and enhanced performant queries.

The command can also function as an ETL process where the original datasource will be transformed and ingested into the output view using the ppl transformation and aggregation operators

**### Syntax
`VIEW (IF NOT EXISTS)? viewName (USING datasource)? (OPTIONS optionsList)? (PARTITIONED BY partitionColumnNames)? location?`

- **viewName**
Specifies a view name, which may be optionally qualified with a database name.

- **USING datasource**
Data Source is the input format used to create the table. Data source can be CSV, TXT, ORC, JDBC, PARQUET, etc.

- **OPTIONS optionsList**
Specifies a set of key-value pairs used to configure the data source. These options vary depending on the chosen data source and may include properties such as file paths, authentication details, format-specific parameters, etc.

- **PARTITIONED BY** 
Specifies the columns on which the data should be partitioned. Partitioning splits the data into separate logical divisions based on distinct values of the specified column(s), which can optimize query performance.

- **location**
Specifies the physical location where the view or table data is stored. This could be a path in a distributed file system like HDFS, S3 Object storage or a local filesystem.

- **QUERY****
The outcome view (viewName) is populated using the data from the select statement.

### Usage Guidelines
The view command produces a view based on the resulting rows returned from the query.
Any query can be used in the `AS <query>` statement and attention must be used to the volume and compute that may incur due to such queries. 

As a precautions an `explain cost | source = table | ... ` can be run prior to the `view` statement to have a better estimation.

### Examples:
```sql
view newTableName using csv | source = table | where fieldA > value | stats count(fieldA) by fieldB

view ipRanges using parquet | source = table | where isV6 = true | eval inRange = case(cidrmatch(ipAddress, '2003:db8::/32'), 'in' else 'out') | fields ip, inRange

view avgBridgesByCountry using json | source = table | fields country, bridges | flatten bridges | fields country, length | stats avg(length) as avg by country

view ageDistribByCountry using parquet partitioned by (age, country)  |
       source = table | stats avg(age) as avg_city_age by country, state, city | eval new_avg_city_age = avg_city_age - 1 | stats 
            avg(new_avg_city_age) as avg_state_age by country, state | where avg_state_age > 18 | stats avg(avg_state_age) as 
            avg_adult_country_age by country

view ageDistribByCountry using parquet OPTIONS('parquet.bloom.filter.enabled'='true', 'parquet.bloom.filter.enabled#age'='false') partitioned by (age, country) |
       source = table | stats avg(age) as avg_city_age by country, state, city | eval new_avg_city_age = avg_city_age - 1 | stats 
            avg(new_avg_city_age) as avg_state_age by country, state | where avg_state_age > 18 | stats avg(avg_state_age) as 
            avg_adult_country_age by country

view ageDistribByCountry using parquet OPTIONS('parquet.bloom.filter.enabled'='true', 'parquet.bloom.filter.enabled#age'='false') partitioned by (age, country)  location 's://demo-app/my-bucket'|
       source = table | stats avg(age) as avg_city_age by country, state, city | eval new_avg_city_age = avg_city_age - 1 | stats 
            avg(new_avg_city_age) as avg_state_age by country, state | where avg_state_age > 18 | stats avg(avg_state_age) as 
            avg_adult_country_age by country

```

### Effective SQL push-down query
The view command is translated into an equivalent SQL `create table <viewName> [Using <datasuorce>] As <statement>` as shown here:

```sql
CREATE TABLE [ IF NOT EXISTS ] table_identifier
    [ ( col_name1 col_type1 [ COMMENT col_comment1 ], ... ) ]
    USING data_source
    [ OPTIONS ( key1=val1, key2=val2, ... ) ]
    [ PARTITIONED BY ( col_name1, col_name2, ... ) ]
    [ CLUSTERED BY ( col_name3, col_name4, ... ) 
        [ SORTED BY ( col_name [ ASC | DESC ], ... ) ] 
        INTO num_buckets BUCKETS ]
    [ LOCATION path ]
    [ COMMENT table_comment ]
    [ TBLPROPERTIES ( key1=val1, key2=val2, ... ) ]
    [ AS select_statement ]
```


```sql
SELECT customer exploded_productId
FROM table
LATERAL VIEW explode(productId) AS exploded_productId
```

### References
- https://spark.apache.org/docs/3.5.3/sql-ref-syntax-ddl-create-table-datasource.html