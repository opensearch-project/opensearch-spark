# Testing PPL using local Spark

## Produce the PPL artifact
The first step would be to produce the spark-ppl artifact: `sbt clean sparkPPLCosmetic/publishM2`

The resulting artifact would be located in the local `.m2/` folder, for example:
`/Users/USER_NAME/.m2/repository/org/opensearch/opensearch-spark-ppl_XX/0.X.0-SNAPSHOT/opensearch-spark-ppl_XX-0.X.0-SNAPSHOT.jar"`

## Downloading spark 3.5.3 version
Download spark from the [official website](https://spark.apache.org/downloads.html) and install locally.

## Start Spark with the plugin
Once installed, run spark with the generated PPL artifact: 
```shell
bin/spark-sql --jars "/PATH_TO_ARTIFACT/opensearch-spark-ppl_XX-0.X.0-SNAPSHOT.jar" \
--conf "spark.sql.extensions=org.opensearch.flint.spark.FlintPPLSparkExtensions"  \
--conf "spark.sql.catalog.dev=org.apache.spark.opensearch.catalog.OpenSearchCatalog" \
--conf "spark.hadoop.hive.cli.print.header=true"

WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
WARN HiveConf: HiveConf of name hive.stats.jdbc.timeout does not exist
WARN HiveConf: HiveConf of name hive.stats.retries.wait does not exist
WARN ObjectStore: Version information not found in metastore. hive.metastore.schema.verification is not enabled so recording the schema version 2.3.0
WARN ObjectStore: setMetaStoreSchemaVersion called but recording version is disabled: version = 2.3.0, comment = Set by MetaStore 
Spark Web UI available at http://*.*.*.*:4040
Spark master: local[*], Application Id: local-1731523264660

spark-sql (default)>
```
The resulting would be a spark-sql prompt: `spark-sql (default)> ...`

### Spark UI Html 
One can also explore spark's UI portal to examine the execution jobs and how they are performing:

![Spark-UX](../img/spark-ui.png)


### Configuring hive partition mode
For simpler configuration of partitioned tables, use the following non-strict mode:

```shell
spark-sql (default)> SET hive.exec.dynamic.partition.mode = nonstrict;
```

---

# Testing PPL Commands

In order to test ppl commands using the spark-sql command line - create and populate the following set of tables:

## emails table
```sql
CREATE TABLE emails (name STRING, age INT, email STRING, street_address STRING, year INT, month INT) PARTITIONED BY (year, month);
INSERT INTO testTable (name, age, email, street_address, year, month) VALUES ('Alice', 30, 'alice@example.com', '123 Main St, Seattle', 2023, 4), ('Bob', 55, 'bob@test.org', '456 Elm St, Portland', 2023, 5), ('Charlie', 65, 'charlie@domain.net', '789 Pine St, San Francisco', 2023, 4), ('David', 19, 'david@anotherdomain.com', '101 Maple St, New York', 2023, 5), ('Eve', 21, 'eve@examples.com', '202 Oak St, Boston', 2023, 4), ('Frank', 76, 'frank@sample.org', '303 Cedar St, Austin', 2023, 5), ('Grace', 41, 'grace@demo.net', '404 Birch St, Chicago', 2023, 4), ('Hank', 32, 'hank@demonstration.com', '505 Spruce St, Miami', 2023, 5), ('Ivy', 9, 'ivy@examples.com', '606 Fir St, Denver', 2023, 4), ('Jack', 12, 'jack@sample.net', '707 Ash St, Seattle', 2023, 5);
```

Now one can run the following ppl commands to test functionality:

### Test `describe` command

```sql
describe emails;

col_name	data_type	comment
name                	string              	                    
age                 	int                 	                    
email               	string              	                    
street_address      	string              	                    
year                	int                 	                    
month               	int                 	                    
# Partition Information	                    	                    
# col_name          	data_type           	comment             
year                	int                 	                    
month               	int                 	                    
                    	                    	                    
# Detailed Table Information	                    	                    
Catalog             	spark_catalog       	                    
Database            	default             	                    
Table               	testtable           	                    
Owner               	lioperry            	                    
Created Time        	Wed Nov 13 14:45:12 MST 2024	                    
Last Access         	UNKNOWN             	                    
Created By          	Spark 3.5.3         	                    
Type                	MANAGED             	                    
Provider            	hive                	                    
Table Properties    	[transient_lastDdlTime=1731534312]	                    
Location            	file:/Users/USER/tools/spark-3.5.3-bin-hadoop3/bin/spark-warehouse/testtable	                    
Serde Library       	org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe	                    
InputFormat         	org.apache.hadoop.mapred.TextInputFormat	                    
OutputFormat        	org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat	                    
Storage Properties  	[serialization.format=1]	                    
Partition Provider  	Catalog             	                    

Time taken: 0.128 seconds, Fetched 28 row(s)
```

### Test `grok` command
```sql
source=emails| grok email '.+@%{HOSTNAME:host}' | fields email, host;

email	host
hank@demonstration.com	demonstration.com
bob@test.org	test.org
jack@sample.net	sample.net
frank@sample.org	sample.org
david@anotherdomain.com	anotherdomain.com
grace@demo.net	demo.net
alice@example.com	example.com
ivy@examples.com	examples.com
eve@examples.com	examples.com
charlie@domain.net	domain.net

Time taken: 0.626 seconds, Fetched 10 row(s)
```

```sql
 source=emails| parse email '.+@(?<host>.+)' | where age > 45 | sort - age | fields age, email, host; 

age	email	host
76	frank@sample.org	sample.org
65	charlie@domain.net	domain.net
55	bob@test.org	test.org

Time taken: 1.555 seconds, Fetched 3 row(s)
```

### Test `fieldsummary` command

```sql
source=emails| fieldsummary includefields=age, email;

Field	COUNT	DISTINCT	MIN	MAX	AVG	MEAN	STDDEV	Nulls	TYPEOF
age	10	10	9	76	36.0	36.0	22.847319317591726	0	int
email	10	10	alice@example.com	jack@sample.net	NULL	NULL	NULL	0	string

Time taken: 1.535 seconds, Fetched 2 row(s)
```

### Test `trendline` command

```sql
source=email | sort - age | trendline sma(2, age);

name	age	email	street_address	year	month	age_trendline
Frank	76	frank@sample.org	303 Cedar St, Austin	2023	5	NULL
Charlie	65	charlie@domain.net	789 Pine St, San Francisco	2023	4	70.5
Bob	55	bob@test.org	456 Elm St, Portland	2023	5	60.0
Grace	41	grace@demo.net	404 Birch St, Chicago	2023	4	48.0
Hank	32	hank@demonstration.com	505 Spruce St, Miami	2023	5	36.5
Alice	30	alice@example.com	123 Main St, Seattle	2023	4	31.0
Eve	21	eve@examples.com	202 Oak St, Boston	2023	4	25.5
David	19	david@anotherdomain.com	101 Maple St, New York	2023	5	20.0
Jack	12	jack@sample.net	707 Ash St, Seattle	2023	5	15.5
Ivy	9	ivy@examples.com	606 Fir St, Denver	2023	4	10.5

Time taken: 1.048 seconds, Fetched 10 row(s)
```


---