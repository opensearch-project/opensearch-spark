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

#### Supported
The next samples of PPL queries are currently supported:

**Fields**
 - `source = table`
 - `source = table | fields a,b,c`

**Filters**
 - `source = table | where a = 1 | fields a,b,c`
 - `source = table | where a >= 1 | fields a,b,c`
 - `source = table | where a < 1 | fields a,b,c`
 - `source = table | where b != 'test' | fields a,b,c`
 - `source = table | where c = 'test' | fields a,b,c | head 3`

**Filters With Logical Conditions**
 - `source = table | where c = 'test' AND a = 1 | fields a,b,c`
 - `source = table | where c != 'test' OR a > 1 | fields a,b,c | head 1`
 - `source = table | where c = 'test' NOT a > 1 | fields a,b,c`

**Aggregations**
 - `source = table | stats avg(a) `
 - `source = table | where a < 50 | stats avg(c) `
 - `source = table | stats max(c) by b`
 - `source = table | stats count(c) by b | head 5`

**Aggregations With Span**
- `source = table  | stats count(a) by span(a, 10) as a_span`
- `source = table  | stats sum(age) by span(age, 5) as age_span | head 2`
- `source = table  | stats avg(age) by span(age, 20) as age_span, country  | sort - age_span |  head 2`

**Aggregations With TimeWindow Span (tumble windowing function)**
- `source = table | stats sum(productsAmount) by span(transactionDate, 1d) as age_date | sort age_date`
- `source = table | stats sum(productsAmount) by span(transactionDate, 1w) as age_date, productId`

> For additional details, review [FlintSparkPPLTimeWindowITSuite.scala](../integ-test/src/test/scala/org/opensearch/flint/spark/ppl/FlintSparkPPLTimeWindowITSuite.scala)

#### Supported Commands:
 - `search` - [See details](https://github.com/opensearch-project/sql/blob/main/docs/user/ppl/cmd/search.rst)  
 - `where`  - [See details](https://github.com/opensearch-project/sql/blob/main/docs/user/ppl/cmd/where.rst)  
 - `fields` - [See details](https://github.com/opensearch-project/sql/blob/main/docs/user/ppl/cmd/fields.rst)  
 - `head`   - [See details](https://github.com/opensearch-project/sql/blob/main/docs/user/ppl/cmd/head.rst)
 - `stats`  - [See details](https://github.com/opensearch-project/sql/blob/main/docs/user/ppl/cmd/stats.rst) (supports AVG, COUNT, MAX, MIN and SUM aggregation functions)
 - `sort` -   [See details](https://github.com/opensearch-project/sql/blob/main/docs/user/ppl/cmd/sort.rst)
 - `correlation` - [See details](../docs/PPL-Correlation-command.md)

> For additional details, review [Integration Tests](../integ-test/src/test/scala/org/opensearch/flint/spark/)
 
---

#### Planned Support

 - support the `explain` command to return the explained PPL query logical plan and expected execution plan

 - attend [sort](https://github.com/opensearch-project/sql/blob/main/docs/user/ppl/cmd/sort.rst) partially supported, missing capability to sort by alias field (span like or aggregation)
 - attend `alias` - partially supported, missing capability to sort by / group-by alias field name 

 - add [conditions](https://github.com/opensearch-project/sql/blob/main/docs/user/ppl/functions/condition.rst) support
 - add [top](https://github.com/opensearch-project/sql/blob/main/docs/user/ppl/cmd/top.rst) support
 - add [cast](https://github.com/opensearch-project/sql/blob/main/docs/user/ppl/functions/conversion.rst) support
 - add [math](https://github.com/opensearch-project/sql/blob/main/docs/user/ppl/functions/math.rst) support
 - add [deduplicate](https://github.com/opensearch-project/sql/blob/main/docs/user/ppl/cmd/dedup.rst) support