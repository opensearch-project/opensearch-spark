
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
For the PPL planned release content [release-plan](planning/release-plan.md)
---
### Commands Specifications


* **Commands**

    - [`explain command `](PPL-Example-Commands.md/#explain)
  
    - [`dedup command `](commands/ppl-dedup-command.md)

    - [`describe command`](PPL-Example-Commands.md/#describe)

    - [`eval command`](commands/ppl-eval-command.md)

    - [`fields command`](commands/ppl-fields-command.md)

    - [`grok command`](commands/ppl-grok-command.md)

    - [`parse command`](commands/ppl-parse-command.md)

    - [`patterns command`](commands/ppl-patterns-command.md)

    - [`rename command`](commands/ppl-rename-command.md)

    - [`search command`](commands/ppl-search-command.md)

    - [`sort command`](commands/ppl-sort-command.md)

    - [`stats command`](commands/ppl-stats-command.md)

    - [`where command`](commands/ppl-where-command.md)

    - [`head command`](commands/ppl-head-command.md)

    - [`rare command`](commands/ppl-rare-command.md)

    - [`top command`](commands/ppl-top-command.md)

    - [`join commands`](commands/ppl-join-command.md)
  
    - [`lookup commands`](commands/ppl-lookup-command.md)
  
    - [`subquery commands`](commands/ppl-subquery-command.md)
  
    - [`correlation commands`](commands/ppl-correlation-command.md)


* **Functions**

    - [`Expressions`](functions/ppl-expressions.md)

    - [`Math Functions`](functions/ppl-math.md)

    - [`Date and Time Functions`](functions/ppl-datetime.md) 

    - [`String Functions`](functions/ppl-string.md)

    - [`Condition Functions`](functions/ppl-condition.md)

    - [`Type Conversion Functions`](functions/ppl-conversion.md)


---
### PPL On Spark

[Running PPL On Spark](PPL-on-Spark.md) gives a basic overview of the PPL language an how it functions on top of Spark


---
### Example PPL Queries
See samples of [PPL queries](PPL-Example-Commands.md) 

---
### Planned PPL Commands

 - [`FillNull`](planning/ppl-fillnull-command.md)

---
### PPL Project Roadmap
[PPL Github Project Roadmap](https://github.com/orgs/opensearch-project/projects/214)