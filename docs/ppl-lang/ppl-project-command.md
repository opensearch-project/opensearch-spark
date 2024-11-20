## PPL `project` command

### Description
Using `project` command to materialize a query into a dedicated view:
In some cases it is required to construct a projection view (materialized into a view) of the query results.
This projection can be later used as a source of continued queries for further slicing and dicing the data, in addition such tables can be also saved into a MV table that are pushed into OpenSearch and can be used for visualization and enhanced performant queries.

The command can also function as an ETL process where the original datasource will be transformed and ingested into the output projected view using the ppl transformation and aggregation operators

### Syntax
`project <viewName> [using datasource] As <query>`

- **viewName**
Specifies a view name, which may be optionally qualified with a database name.

- **USING datasource**
Data Source is the input format used to create the table. Data source can be CSV, TXT, ORC, JDBC, PARQUET, etc.

- **AS query**

The table is populated using the data from the select statement.

### Usage Guidelines
The project command produces a view based on the resulting rows returned from the query.
Any query can be used in the `AS <query>` statement and attention must be used to the volume and compute that may incur due to such queries. 

As a precautions an `explain cost | source = table | ... ` can be run prior to the `project` statement to have a better estimation.

### Examples:
```sql
project newTableName as |
   source = table | where fieldA > value | stats count(fieldA) by fieldB

project ipRanges as |
       source = table | where isV6 = true | eval inRange = case(cidrmatch(ipAddress, '2003:db8::/32'), 'in' else 'out') | fields ip, inRange

project avgBridgesByCountry as |
       source = table | fields country, bridges | flatten bridges | fields country, length | stats avg(length) as avg by country

project ageDistribByCountry as |
       source = table | stats avg(age) as avg_city_age by country, state, city | eval new_avg_city_age = avg_city_age - 1 | stats 
            avg(new_avg_city_age) as avg_state_age by country, state | where avg_state_age > 18 | stats avg(avg_state_age) as 
            avg_adult_country_age by country

```

### Effective SQL push-down query
The project command is translated into an equivalent SQL `create table <viewName> [Using <datasuorce>] As <statement>` as shown here:

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