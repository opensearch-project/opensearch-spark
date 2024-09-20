## PPL Language Support On Spark 

This module provides the support for running [PPL](https://github.com/opensearch-project/piped-processing-language) queries on Spark using direct logical plan 
translation between PPL's logical plan to Spark's Catalyst logical plan.

### Context
The next concepts are the main purpose of introduction this functionality:
- Transforming PPL to become OpenSearch default query language (specifically for logs/traces/metrics signals)
- Promoting PPL as a viable candidate for the proposed  CNCF Observability universal query language.
- Seamlessly Interact with different datasources such as S3 / Prometheus / data-lake leveraging spark execution.
- Using spark's federative capabilities as a general purpose query engine to facilitate complex queries including joins
- Improve and promote PPL to become extensible and general purpose query language to be adopted by the community


Acknowledging spark is an excellent conduit for promoting these goals and showcasing the capabilities of PPL to interact & federate data across multiple sources and domains.

Another byproduct of introducing PPL on spark would be the much anticipated JOIN capability that will emerge from the usage of Spark compute engine.

**What solution would you like?**

For PPL to become a library which has a simple and easy means of importing and extending, PPL client (the thin API layer) which can interact and provide a generic query composition framework to be used in any type of application independently of OpenSearch plugins.

![PPL endpoint](https://github.com/opensearch-project/opensearch-spark/assets/48943349/e9831a8f-abde-484c-9c62-331570e88460)

As depicted in the above image, the protocol & AST (antler based language traversals ) verticals should be detached and composed into a self sustainable component that can be imported regardless of OpenSearch plugins.

---

## PPL On Spark

Running PPL on spark is a goal for allowing simple adoption of PPL query language and also for simplifying the Flint project to allow visualization for federated queries using the Observability dashboards capabilities.


### Background

In Apache Spark, the DataFrame API serves as a programmatic interface for data manipulation and queries, allowing the construction of complex operations using a chain of method calls. This API can work in tandem with other query languages like SQL or PPL.

For instance, if you have a PPL query and a translator, you can convert it into DataFrame operations to generate an optimized execution plan. Spark's underlying Catalyst optimizer will convert these DataFrame transformations and actions into an optimized physical plan executed over RDDs or Datasets.

The following section describes the two main options for translating the PPL query (using the logical plan) into the spark corespondent component (either dataframe API or spark logical plan)


### Translation Process

**Using Catalyst Logical Plan Grammar**
The leading option for translation would be using the Catalyst Grammar for directly translating the Logical plan steps
Here is an example of such translation outcome:


Our goal would be translating the PPL into the Unresolved logical plan so that the Analysis phase would behave in the similar manner to the SQL originated query.

![spark execution process](https://github.com/opensearch-project/opensearch-spark/assets/48943349/780c0072-0ab4-4fb4-afb1-11fb3bfbd2c3)

**The following PPL query:**
`search source=t'| where a=1`

Translates into the PPL logical plan:
`Relation(tableName=t, alias=null), Compare(operator==, left=Field(field=a, fieldArgs=[]), right=1)`

Would be transformed into the next catalyst Plan:
```
// Create an UnresolvedRelation for the table 't'
val table = UnresolvedRelation(TableIdentifier("t"))
// Create an EqualTo expression for "a == 1" 
val equalToCondition = EqualTo(UnresolvedAttribute("a"), ..Literal(1))
// Create a Filter LogicalPlan
val filterPlan = Filter(equalToCondition, table) 
```

The following PPL query:
`source=t | stats count(a) by b`

Would produce the next PPL Logical Plan":
```
Aggregation(aggExprList=[Alias(name=count(a), delegated=count(Field(field=a, fieldArgs=[])), alias=null)], 
sortExprList=[], groupExprList=[Alias(name=b, delegated=Field(field=b, fieldArgs=[]), alias=null)], span=null, argExprList=[Argument(argName=partitions, value=1), Argument(argName=allnum, value=false), Argument(argName=delim, value= ), Argument(argName=dedupsplit, value=false)], child=[Relation(tableName=t, alias=null)])
```

Would be transformed into the next catalyst Plan:
```
// Create an UnresolvedRelation for the table 't'
 val table = UnresolvedRelation(TableIdentifier("t"))
 // Create an Alias for the aggregation expression 'count(a)' 
val aggExpr = Alias(Count(UnresolvedAttribute("a")), "count(a)")() 
// Create an Alias for the grouping expression 'b' 
val groupExpr = Alias(UnresolvedAttribute("b"), "b")() 
// Create an Aggregate LogicalPlan val aggregatePlan = Aggregate(Seq(groupExpr), Seq(groupExpr, aggExpr), table) 
```

---


## Design Considerations

In general when translating between two query languages we have the following options:

**1) Source Grammar Tree To destination Dataframe API Translation**
This option uses the syntax tree to directly translate from one language syntax grammar tree to the other language (dataframe) API thus eliminating the parsing phase and creating a strongly validated process that can be verified and tested with high degree of confidence.

**Advantages :**
- Simpler solution to develop since the abstract structure of the query language is simpler to transform into compared with other transformation options. -using the build-in traverse visitor API
- Optimization potential by leveraging the specific knowledge of the actual original language and  being able to directly use specific grammar function and commands directly.

**Disadvantages :**
- Fully depended on the Source Code of the target language including potentially internal structure of its grammatical components - In spark case this is not a severe disadvantage since this is a very well know and well structured API grammar.
- Not sufficiently portable since this api is coupled with the

**2) Source Logical Plan To destination Logical Plan (Catalyst) [Preferred Option]**
This option uses the syntax tree to directly translate from one language syntax grammar tree to the other language syntax grammar tree thus eliminating the parsing phase and creating a strongly validated process that can be verified and tested with high degree of confidence.

Once the target plan is created - it can be analyzed and executed separately from the translations process (or location)

```
  SparkSession spark = SparkSession.builder()
                .appName("SparkExecuteLogicalPlan")
                .master("local")
                .getOrCreate();

        // catalyst logical plan - translated from PPL Logical plan
        Seq<NamedExpression> scalaProjectList = //... your project list
        LogicalPlan unresolvedTable = //... your unresolved table
        LogicalPlan projectNode = new Project(scalaProjectList, unresolvedTable);

        // Analyze and execute
        Analyzer analyzer = new Analyzer(spark.sessionState().catalog(), spark.sessionState().conf());
        LogicalPlan analyzedPlan = analyzer.execute(projectNode);
        LogicalPlan optimizedPlan = spark.sessionState().optimizer().execute(analyzedPlan);

        QueryExecution qe = spark.sessionState().executePlan(optimizedPlan);
        Dataset<Row> result = new Dataset<>(spark, qe, RowEncoder.apply(qe.analyzed().schema()));

```
**Advantages :**
- A little more complex develop compared to the first option but still relatively simple since the abstract structure of the query language is simpler to transform into another’s language syntax grammar tree

- Optimization potential by leveraging the specific knowledge of the actual original language and  being able to directly use specific grammar function and commands directly.

**Disadvantages :**
- Fully depended on the Source Code of the target language including potentially internal structure of its grammatical components - In spark case this is not a severe disadvantage since this is a very well know and well structured API grammar.
- Add the additional phase for analyzing the logical plan and generating the physical plan and the execution part itself.


**3) Source Grammar Tree To destination Query Translation**
This option uses the syntax tree to from the original query language into the target query (SQL in our case). This is a more generalized solution that may be utilized for additional purposes such as direct query to an RDBMS server.

**Advantages :**
- A general purpose solution that may be utilized for other SQL compliant servers

**Disadvantages :**
- This is a more complicated use case since it requires additional layer of complexity to be able to correctly translate the original syntax tree to a textual representation of the outcome language that has to be parsed and verified
- SQL plugin already support SQL so its not clear what is the advantage of translating PPL back to SQL since our plugin already supports SQL out of the box.

---
### Architecture

**1. Using Spark Connect (PPL Grammar To dataframe API Translation)**

In Apache Spark 3.4, Spark Connect introduced a decoupled client-server architecture that allows remote connectivity to Spark clusters using the DataFrame API and unresolved logical plans as the protocol.

**How Spark Connect works**:
The Spark Connect client library is designed to simplify Spark application development. It is a thin API that can be embedded everywhere: in application servers, IDEs, notebooks, and programming languages. The Spark Connect API builds on Spark’s DataFrame API using unresolved logical plans as a language-agnostic protocol between the client and the Spark driver.

The Spark Connect client translates DataFrame operations into unresolved logical query plans which are encoded using protocol buffers. These are sent to the server using the gRPC framework.
The Spark Connect endpoint embedded on the Spark Server receives and translates unresolved logical plans into Spark’s logical plan operators. This is similar to parsing a SQL query, where attributes and relations are parsed and an initial parse plan is built. From there, the standard Spark execution process kicks in, ensuring that Spark Connect leverages all of Spark’s optimizations and enhancements. Results are streamed back to the client through gRPC as Apache Arrow-encoded row batches.

**Advantages :**
Stability: Applications that use too much memory will now only impact their own environment as they can run in their own processes. Users can define their own dependencies on the client and don’t need to worry about potential conflicts with the Spark driver.

Upgradability: The Spark driver can now seamlessly be upgraded independently of applications, for example to benefit from performance improvements and security fixes. This means applications can be forward-compatible, as long as the server-side RPC definitions are designed to be backwards compatible.

Debuggability and observability: Spark Connect enables interactive debugging during development directly from your favorite IDE. Similarly, applications can be monitored using the application’s framework native metrics and logging libraries.

Not need separating PPL into a dedicated library - all can be done from the existing SQL repository.

**Disadvantages :**
Not all _managed_ Spark solution support this "new" feature so as part of using this capability we will need to manually deploy the corresponding spark-connect plugins as part of flint’s deployment.

All the context creation would have to be done from the spark client - this creates some additional complexity since the Flint spark plugin has some contextual requirements that have to be somehow propagated from the client’s side .

---

### Implemented solution
As presented here and detailed in the [issue](https://github.com/opensearch-project/opensearch-spark/issues/30), there are several options to allow spark to be able to understand and run ppl queries.

The selected option is to us the PPL AST logical plan API and traversals to transform the PPL logical plan into Catalyst logical plan thus enabling a the longer term
solution for using spark-connect as a part of the ppl-client (as described below): 

Advantages of the selected approach:

- **reuse** of existing PPL code that is tested and in production
- **simplify** development while relying on well known and structured codebase
- **long term support** in case the `spark-connect` will become user chosen strategy - existing code can be used without any changes
- **single place of maintenance**  by reusing the PPL logical model which relies on ppl antlr parser, we can use a single repository to maintain and develop the PPL language without the need to constantly merge changes upstream .

The following diagram shows the high level architecture of the selected implementation solution :

![ppl logical architecture ](https://github.com/opensearch-project/opensearch-spark/assets/48943349/6965258f-9823-4f12-a4f9-529c1365fc4a)

The **logical Architecture**  show the next artifacts:
- **_Libraries_**:
    - PPL ( the ppl core , protocol, parser & logical plan utils)
    - SQL ( the SQL core , protocol, parser - depends on PPL for using the logical plan utils)

- **_Drivers_**:
    -  PPL OpenSearch Driver (depends on OpenSearch core)
    -  PPL Spark Driver (depends on Spark core)
    -  PPL Prometheus Driver (directly translates PPL to PromQL )
    -  SQL OpenSearch Driver (depends on OpenSearch core)

**Physical Architecture :**
Currently the drivers reside inside the PPL client repository within the OpenSearch Plugins.
Next tasks ahead will resolve this:

- Extract PPL logical component outside the SQL plugin into a (none-plugin) library - publish library to maven
- Separate the PPL / SQL drivers inside the OpenSearch PPL client to better distinguish
- Create a thin PPL client capable of interaction with the PPL Driver regardless of which driver (Spark , OpenSearch , Prometheus )

---

### Roadmap

This section describes the next steps planned for enabling additional commands and gamer translation.

#### Example PPL Queries
See the next samples of PPL queries :

**Describe**
 - `describe table`  This command is equal to the `DESCRIBE EXTENDED table` SQL command

**Fields**
 - `source = table`
 - `source = table | fields a,b,c`

**Nested-Fields**
 - `source = catalog.schema.table1, catalog.schema.table2 | fields A.nested1, B.nested1`
 - `source = catalog.table | where struct_col2.field1.subfield > 'valueA' | sort int_col | fields  int_col, struct_col.field1.subfield, struct_col2.field1.subfield`
 - `source = catalog.schema.table | where struct_col2.field1.subfield > 'valueA' | sort int_col | fields  int_col, struct_col.field1.subfield, struct_col2.field1.subfield`

**Filters**
 - `source = table | where a = 1 | fields a,b,c`
 - `source = table | where a >= 1 | fields a,b,c`
 - `source = table | where a < 1 | fields a,b,c`
 - `source = table | where b != 'test' | fields a,b,c`
 - `source = table | where c = 'test' | fields a,b,c | head 3`
 - `source = table | where ispresent(b)`
 - `source = table | where isnull(coalesce(a, b)) | fields a,b,c | head 3`
 - `source = table | where isempty(a)`
 - `source = table | where case(length(a) > 6, 'True' else 'False') = 'True'`;

**Filters With Logical Conditions**
 - `source = table | where c = 'test' AND a = 1 | fields a,b,c`
 - `source = table | where c != 'test' OR a > 1 | fields a,b,c | head 1`
 - `source = table | where c = 'test' NOT a > 1 | fields a,b,c`


**Eval**

Assumptions: `a`, `b`, `c` are existing fields in `table`
 - `source = table | eval f = 1 | fields a,b,c,f`
 - `source = table | eval f = 1` (output a,b,c,f fields)
 - `source = table | eval n = now() | eval t = unix_timestamp(a) | fields n,t`
 - `source = table | eval f = a | where f > 1 | sort f | fields a,b,c | head 5`
 - `source = table | eval f = a * 2 | eval h = f * 2 | fields a,f,h`
 - `source = table | eval f = a * 2, h = f * 2 | fields a,f,h`
 - `source = table | eval f = a * 2, h = b | stats avg(f) by h`
 - `source = table | eval f = ispresent(a)`
 - `source = table | eval r = coalesce(a, b, c) | fields r`
 - `source = table | eval e = isempty(a) | fields e`
   ```
   source = table | eval e = eval status_category =
   case(a >= 200 AND a < 300, 'Success',
   a >= 300 AND a < 400, 'Redirection',
   a >= 400 AND a < 500, 'Client Error',
   a >= 500, 'Server Error'
   else 'Unknown'
   )
   ```

Limitation: Overriding existing field is unsupported, following queries throw exceptions with "Reference 'a' is ambiguous" 
 - `source = table | eval a = 10 | fields a,b,c`
 - `source = table | eval a = a * 2 | stats avg(a)`
 - `source = table | eval a = abs(a) | where a > 0`

**Aggregations**
 - `source = table | stats avg(a) `
 - `source = table | where a < 50 | stats avg(c) `
 - `source = table | stats max(c) by b`
 - `source = table | stats count(c) by b | head 5`
 - `source = table | stats distinct_count(c)`
 - `source = table | stats stddev_samp(c)`
 - `source = table | stats stddev_pop(c)`
 - `source = table | stats percentile(c, 90)`
 - `source = table | stats percentile_approx(c, 99)`

**Aggregations With Span**
- `source = table  | stats count(a) by span(a, 10) as a_span`
- `source = table  | stats sum(age) by span(age, 5) as age_span | head 2`
- `source = table  | stats avg(age) by span(age, 20) as age_span, country  | sort - age_span |  head 2`

**Aggregations With TimeWindow Span (tumble windowing function)**
- `source = table | stats sum(productsAmount) by span(transactionDate, 1d) as age_date | sort age_date`
- `source = table | stats sum(productsAmount) by span(transactionDate, 1w) as age_date, productId`

**Aggregations Group by Multiple Levels**
- `source = table | stats avg(age) as avg_state_age by country, state | stats avg(avg_state_age) as avg_country_age by country`
- `source = table | stats avg(age) as avg_city_age by country, state, city | eval new_avg_city_age = avg_city_age - 1 | stats avg(new_avg_city_age) as avg_state_age by country, state | where avg_state_age > 18 | stats avg(avg_state_age) as avg_adult_country_age by country`

**Dedup**
- `source = table | dedup a | fields a,b,c`
- `source = table | dedup a,b | fields a,b,c`
- `source = table | dedup a keepempty=true | fields a,b,c`
- `source = table | dedup a,b keepempty=true | fields a,b,c`
- `source = table | dedup 1 a | fields a,b,c`
- `source = table | dedup 1 a,b | fields a,b,c`
- `source = table | dedup 1 a keepempty=true | fields a,b,c`
- `source = table | dedup 1 a,b keepempty=true | fields a,b,c`
- `source = table | dedup 2 a | fields a,b,c`
- `source = table | dedup 2 a,b | fields a,b,c`
- `source = table | dedup 2 a keepempty=true | fields a,b,c`
- `source = table | dedup 2 a,b keepempty=true | fields a,b,c`
- `source = table | dedup 1 a consecutive=true| fields a,b,c` (Consecutive deduplication is unsupported)

**Rare**
- `source=accounts | rare gender`
- `source=accounts | rare age by gender`

**Top**
- `source=accounts | top gender`
- `source=accounts | top 1 gender`
- `source=accounts | top 1 age by gender`

**Parse**
- `source=accounts | parse email '.+@(?<host>.+)' | fields email, host `
- `source=accounts | parse email '.+@(?<host>.+)' | top 1 host `
- `source=accounts | parse email '.+@(?<host>.+)' | stats count() by host`
- `source=accounts | parse email '.+@(?<host>.+)' | eval eval_result=1 | fields host, eval_result`
- `source=accounts | parse email '.+@(?<host>.+)' | where age > 45 | sort - age | fields age, email, host`
- `source=accounts | parse address '(?<streetNumber>\d+) (?<street>.+)' | where streetNumber > 500 | sort num(streetNumber) | fields streetNumber, street`

**Grok**
- `source=accounts | grok email '.+@%{HOSTNAME:host}' | top 1 host`
- `source=accounts | grok email '.+@%{HOSTNAME:host}' | stats count() by host`
- `source=accounts | grok email '.+@%{HOSTNAME:host}' | eval eval_result=1 | fields host, eval_result`
- `source=accounts | grok email '.+@%{HOSTNAME:host}' | eval eval_result=1 | fields host, eval_result`
- `source=accounts | grok street_address '%{NUMBER} %{GREEDYDATA:address}' | fields address `
- `source=logs | grok message '%{COMMONAPACHELOG}' | fields COMMONAPACHELOG, timestamp, response, bytes`

**Patterns**
- `source=accounts | patterns email | fields email, patterns_field `
- `source=accounts | patterns email | where age > 45 | sort - age | fields email, patterns_field`
- `source=apache | patterns new_field='no_numbers' pattern='[0-9]' message | fields message, no_numbers`
- `source=apache | patterns new_field='no_numbers' pattern='[0-9]' message | stats count() by no_numbers`

_- **Limitation: Overriding existing field is unsupported:**_
  - `source=accounts | grok address '%{NUMBER} %{GREEDYDATA:address}' | fields address`

**Join**
- `source = table1 | inner join left = l right = r on l.a = r.a table2 | fields l.a, r.a, b, c`
- `source = table1 | left join left = l right = r on l.a = r.a table2 | fields l.a, r.a, b, c`
- `source = table1 | right join left = l right = r on l.a = r.a table2 | fields l.a, r.a, b, c`
- `source = table1 | full left = l right = r on l.a = r.a table2 | fields l.a, r.a, b, c`
- `source = table1 | cross join left = l right = r table2`
- `source = table1 | left semi join left = l right = r on l.a = r.a table2`
- `source = table1 | left anti join left = l right = r on l.a = r.a table2`

_- **Limitation: sub-searches is unsupported in join right side now**_

Details of Join command, see [PPL-Join-Command](../docs/PPL-Join-command.md)

---
#### Experimental Commands:
- `correlation` - [See details](../docs/PPL-Correlation-command.md)
> This is an experimental command - it may be removed in future versions

---
### Documentations 

For additional details on PPL commands, see [PPL Commands Docs](https://github.com/opensearch-project/sql/blob/main/docs/user/ppl/index.rst)

For additional details on Spark PPL commands project, see [PPL Project](https://github.com/orgs/opensearch-project/projects/214/views/2)

For additional details on Spark PPL commands support campaign, see [PPL Commands Campaign](https://github.com/opensearch-project/opensearch-spark/issues/408)


 