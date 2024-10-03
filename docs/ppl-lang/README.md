
### PPL Language

Overview
---------
Piped Processing Language (PPL), powered by OpenSearch, enables OpenSearch users with exploration and discovery of, and finding search patterns in data stored in OpenSearch, using a set of commands delimited by pipes (|). These are essentially read-only requests to process data and return results.

Currently, OpenSearch users can query data using either Query DSL or SQL. Query DSL is powerful and fast. However, it has a steep learning curve, and was not designed as a human interface to easily create ad hoc queries and explore user data. SQL allows users to extract and analyze data in OpenSearch in a declarative manner. OpenSearch now makes its search and query engine robust by introducing Piped Processing Language (PPL). It enables users to extract insights from OpenSearch with a sequence of commands delimited by pipes (|). It supports  a comprehensive set of commands including search, where, fields, rename, dedup, sort, eval, head, top and rare, and functions, operators and expressions. Even new users who have recently adopted OpenSearch, can be productive day one, if they are familiar with the pipe (|) syntax. It enables developers, DevOps engineers, support engineers, site reliability engineers (SREs), and IT managers to effectively discover and explore log, monitoring and observability data stored in OpenSearch.

We expand the capabilities of our Workbench, a comprehensive and integrated visual query tool currently supporting only SQL, to run on-demand PPL commands, and view and save results as text and JSON. We also add  a new interactive standalone command line tool, the PPL CLI, to run on-demand PPL commands, and view and save results as text and JSON.

The query start with search command and then flowing a set of command delimited by pipe (|).
| for example, the following query retrieve firstname and lastname from accounts if age large than 18.

```sql
source=accounts
| where age > 18
| fields firstname, lastname
```

---
### Specifications


* **Commands**

    - [`explain command `](PPL-Example-Commands.md/#explain)
  
    - [`dedup command `](ppl-dedup-command.md)

    - [`describe command`](PPL-Example-Commands.md/#describe)

    - [`eval command`](ppl-eval-command.md)

    - [`fields command`](ppl-fields-command.md)

    - [`grok command`](ppl-grok-command.md)

    - [`parse command`](ppl-parse-command.md)

    - [`patterns command`](ppl-patterns-command.md)

    - [`rename command`](ppl-rename-command.md)

    - [`search command`](ppl-search-command.md)

    - [`sort command`](ppl-sort-command.md)

    - [`stats command`](ppl-stats-command.md)

    - [`where command`](ppl-where-command.md)

    - [`head command`](ppl-head-command.md)

    - [`rare command`](ppl-rare-command.md)

    - [`top command`](ppl-top-command.md)

    - [`join commands`](ppl-join-command.md)
  
    - [`lookup commands`](ppl-lookup-command.md)
  
    - [`correlation commands`](ppl-correlation-command.md)


* **Functions**

    - `Expressions <functions/expressions.rst>`_

    - `Math Functions <functions/math.rst>`_

    - `Date and Time Functions <functions/datetime.rst>`_

    - `String Functions <functions/string.rst>`_

    - `Condition Functions <functions/condition.rst>`_

    - `Relevance Functions <functions/relevance.rst>`_

    - `Type Conversion Functions <functions/conversion.rst>`_

    - `System Functions <functions/system.rst>`_


---
### PPL On Spark

[Running PPL On Spark](PPL-on-Spark.md) gives a basic overview of the PPL language an how it functions on top of Spark


---
### Example PPL Queries
See samples of [PPL queries](PPL-Example-Commands.md) 

---

### PPL Project Roadmap
[PPL Github Project Roadmap](https://github.com/orgs/opensearch-project/projects/214)