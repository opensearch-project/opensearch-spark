
## PPL Language

## Overview

Piped Processing Language (PPL), powered by OpenSearch, enables OpenSearch users with exploration and discovery of, and finding search patterns in data stored in OpenSearch Or S3.

The PPL query start with search command and then flowing a set of command delimited by pipe (|).
for example, the following query retrieve firstname and lastname from accounts if age larger than 18.

```sql
source=accounts
| where age > 18
| fields firstname, lastname
```

For additional examples see the next [documentation](PPL-Example-Commands.md). 

---
### Commands Specifications


* **Commands**

    - [`comment`](ppl-comment.md)

    - [`explain command `](PPL-Example-Commands.md/#explain)
  
    - [`dedup command `](ppl-dedup-command.md)

    - [`describe command`](PPL-Example-Commands.md/#describe)
  
    - [`fillnull command`](ppl-fillnull-command.md)
     
    - [`flatten command`](ppl-flatten-command.md)

    - [`eval command`](ppl-eval-command.md)

    - [`fields command`](ppl-fields-command.md)

    - [`grok command`](ppl-grok-command.md)

    - [`parse command`](ppl-parse-command.md)

    - [`patterns command`](ppl-patterns-command.md)

    - [`rename command`](ppl-rename-command.md)

    - [`search command`](ppl-search-command.md)

    - [`sort command`](ppl-sort-command.md)

    - [`stats command`](ppl-stats-command.md)

    - [`eventstats command`](ppl-eventstats-command.md)

    - [`where command`](ppl-where-command.md)

    - [`head command`](ppl-head-command.md)

    - [`rare command`](ppl-rare-command.md)

    - [`top command`](ppl-top-command.md)

    - [`join commands`](ppl-join-command.md)
  
    - [`lookup commands`](ppl-lookup-command.md)
  
    - [`subquery commands`](ppl-subquery-command.md)
  
    - [`correlation commands`](ppl-correlation-command.md)
  
    - [`trendline commands`](ppl-trendline-command.md)
  
    - [`expand commands`](ppl-expand-command.md)

    - [`appendcol commands`](ppl-appendcol-command.md)

* **Functions**

    - [`Expressions`](functions/ppl-expressions.md)

    - [`Math Functions`](functions/ppl-math.md)

    - [`Date and Time Functions`](functions/ppl-datetime.md) 

    - [`String Functions`](functions/ppl-string.md)

    - [`JSON Functions`](functions/ppl-json.md)

    - [`Condition Functions`](functions/ppl-condition.md)

    - [`Type Conversion Functions`](functions/ppl-conversion.md)

    - [`Cryptographic Functions`](functions/ppl-cryptographic.md)

    - [`IP Address Functions`](functions/ppl-ip.md)
     
    - [`Collection Functions`](functions/ppl-collection)

---
### PPL On Spark

[Running PPL On Spark](PPL-on-Spark.md) gives a basic overview of the PPL language an how it functions on top of Spark


---
### Example PPL Queries
See samples of [PPL queries](PPL-Example-Commands.md) 

---

### Experiment PPL locally using Spark-Cluster
See ppl usage sample on local spark cluster[PPL on local spark ](local-spark-ppl-test-instruction.md) 

---
### TPC-H PPL Query Rewriting
See samples of [TPC-H PPL query rewriting](ppl-tpch.md)

---
### Planned PPL Commands

 - [`FillNull`](planning/ppl-fillnull-command.md)

---
### PPL Project Roadmap
[PPL Github Project Roadmap](https://github.com/orgs/opensearch-project/projects/214)
