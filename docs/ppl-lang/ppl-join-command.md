## PPL Join Command

## Overview

[Trace analytics](https://opensearch.org/docs/latest/observability-plugin/trace/ta-dashboards/) considered using SQL/PPL for its queries, but some graphs rely on joining two indices (span index and service map index) together which is not supported by SQL/PPL. Trace analytics was implemented with DSL + javascript, would be good if `join` being added to SQL could support this use case.

### Schema

There will be at least 2 indices, `otel-v1-apm-span-*` (large) and `otel-v1-apm-service-map` (small).

Relevant fields from indices:

`otel-v1-apm-span-*`:

- traceId - A unique identifier for a trace. All spans from the same trace share the same traceId.
- spanId - A unique identifier for a span within a trace, assigned when the span is created.
- parentSpanId - The spanId of this span's parent span. If this is a root span, then this field must be empty.
- durationInNanos - Difference in nanoseconds between startTime and endTime. (this is `latency` in UI)
- serviceName - The resource from the span originates.
- traceGroup - The name of the trace's root span.

`otel-v1-apm-service-map`:

- serviceName - The name of the service which emitted the span.
- destination.domain - The serviceName of the service being called by this client.
- destination.resource - The span name (API, operation, etc.) being called by this client.
- target.domain - The serviceName of the service being called by a client.
- target.resource - The span name (API, operation, etc.) being called by a client.
- traceGroupName - The top-level span name which started the request chain.

Full schemas are defined in data-prepper repo: [`otel-v1-apm-span-*`](https://github.com/opensearch-project/data-prepper/blob/04dd7bd18977294800cf4b77d7f01914def75f23/docs/schemas/trace-analytics/otel-v1-apm-span-index-template.md), [`otel-v1-apm-service-map`](https://github.com/opensearch-project/data-prepper/blob/4e5f83814c4a0eed2a1ca9bab0693b9e32240c97/docs/schemas/trace-analytics/otel-v1-apm-service-map-index-template.md)

### Requirement

Support `join` to calculate the following:

For each service, join span index on service map index to calculate metrics under different type of filters.

![image](https://user-images.githubusercontent.com/28062824/194170062-f0dd1d57-c5eb-44db-95e0-6b3b4e52f25a.png)

This sample query calculates latency when filtered by trace group `client_cancel_order` for the `order` service. I only have a subquery example, don't have the join version of the query..

```sql
SELECT avg(durationInNanos)
FROM `otel-v1-apm-span-000001` t1
WHERE t1.serviceName = `order`
  AND ((t1.name in
          (SELECT target.resource
           FROM `otel-v1-apm-service-map`
           WHERE serviceName = `order`
             AND traceGroupName = `client_cancel_order`)
        AND t1.parentSpanId != NULL)
       OR (t1.parentSpanId = NULL
           AND t1.name = `client_cancel_order`))
  AND t1.traceId in
    (SELECT traceId
     FROM `otel-v1-apm-span-000001`
     WHERE serviceName = `order`)
```
## Migrate to PPL

### Syntax of Join Command

```sql
SEARCH source=<left-table>
| <other piped command>
| [joinType] JOIN
    [leftAlias]
    [rightAlias]
    [joinHints]
    ON joinCriteria
    <right-table>
| <other piped command>
```
**joinType**
- Syntax: `[INNER] | LEFT [OUTER] | RIGHT [OUTER] | FULL [OUTER] | CROSS | [LEFT] SEMI | [LEFT] ANTI`
- Optional
- Description: The type of join to perform. The default is `INNER` if not specified.

**leftAlias**
- Syntax: `left = <leftAlias>`
- Optional
- Description: The subquery alias to use with the left join side, to avoid ambiguous naming.

**rightAlias**
- Syntax: `right = <rightAlias>`
- Optional
- Description: The subquery alias to use with the right join side, to avoid ambiguous naming.

**joinHints**
- Syntax: `[hint.left.key1 = value1 hint.right.key2 = value2]`
- Optional
- Description: Zero or more space-separated join hints in the form of `Key` = `Value`. The key must start with `hint.left.` or `hint.right.`

**joinCriteria**
- Syntax: `<expression>`
- Required
- Description: The syntax starts with `ON`. It could be any comparison expression. Generally, the join criteria looks like `<leftAlias>.<leftField>=<rightAlias>.<rightField>`. For example: `l.id = r.id`. If the join criteria contains multiple conditions, you can specify `AND` and `OR` operator between each comparison expression. For example, `l.id = r.id AND l.email = r.email AND (r.age > 65 OR r.age < 18)`.

**right-table**
- Required
- Description: The index or table name of join right-side. Sub-search is unsupported in join right side for now.

### Rewriting
```sql
SEARCH source=otel-v1-apm-span-000001
| WHERE serviceName = 'order'
| JOIN left=t1 right=t2
    ON t1.traceId = t2.traceId AND t2.serviceName = 'order'
    otel-v1-apm-span-000001 -- self inner join
| EVAL s_name = t1.name -- rename to avoid ambiguous
| EVAL s_parentSpanId = t1.parentSpanId -- RENAME command would be better when it is supported
| EVAL s_durationInNanos = t1.durationInNanos 
| FIELDS s_name, s_parentSpanId, s_durationInNanos -- reduce colunms in join
| LEFT JOIN left=s1 right=t3
    ON s_name = t3.target.resource AND t3.serviceName = 'order' AND t3.traceGroupName = 'client_cancel_order'
    otel-v1-apm-service-map
| WHERE (s_parentSpanId IS NOT NULL OR (s_parentSpanId IS NULL AND s_name = 'client_cancel_order'))
| STATS avg(s_durationInNanos) -- no need to add alias if there is no ambiguous
```


### More examples

Migration from SQL query (TPC-H Q13):
```sql
SELECT c_count, COUNT(*) AS custdist
FROM
  ( SELECT c_custkey, COUNT(o_orderkey) c_count
    FROM customer LEFT OUTER JOIN orders ON c_custkey = o_custkey
        AND o_comment NOT LIKE '%unusual%packages%'
    GROUP BY c_custkey
  ) AS c_orders
GROUP BY c_count
ORDER BY custdist DESC, c_count DESC;
```
Rewritten by PPL Join query:
```sql
SEARCH source=customer
| FIELDS c_custkey
| LEFT OUTER JOIN
    ON c_custkey = o_custkey AND o_comment NOT LIKE '%unusual%packages%'
    orders
| STATS count(o_orderkey) AS c_count BY c_custkey
| STATS count() AS custdist BY c_count
| SORT - custdist, - c_count
```
_- **Limitation: sub-searches is unsupported in join right side**_

If sub-searches is supported, above ppl query could be rewritten as:
```sql
SEARCH source=customer
| FIELDS c_custkey
| LEFT OUTER JOIN
   ON c_custkey = o_custkey
   [
      SEARCH source=orders
      | WHERE o_comment NOT LIKE '%unusual%packages%'
      | FIELDS o_orderkey, o_custkey
   ]
| STATS count(o_orderkey) AS c_count BY c_custkey
| STATS count() AS custdist BY c_count
| SORT - custdist, - c_count
```

### Comparison with [Correlation](ppl-correlation-command)

A primary difference between `correlate` and `join` is that both sides of `correlate` are tables, but both sides of `join` are subqueries. 
For example:
```sql
source = testTable1
 | where country = 'Canada' OR country = 'England'
 | eval cname = lower(name)
 | fields cname, country, year, month
 | inner join left=l, right=r
     ON l.cname = r.name AND l.country = r.country AND l.year = 2023 AND r.month = 4
     testTable2s
```
The subquery alias `l` does not represent the `testTable1` table itself. Instead, it represents the subquery:
```sql
source = testTable1
| where country = 'Canada' OR country = 'England'
| eval cname = lower(name)
| fields cname, country, year, month
```
Therefore, the condition of `join` must be `subqueryAlias.field = subqueryAlias.field`, rather than `tableName.field = tableName.field` as in `correlate`.
