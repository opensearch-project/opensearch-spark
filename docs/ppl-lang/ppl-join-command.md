## PPL `join` command

### Description

`JOIN` command combines two datasets together. The left side could be an index or results from a piped commands, the right side could be either an index or a subquery.

### Syntax

`[joinType] join [leftAlias] [rightAlias] [joinHints] on <joinCriteria> <right-dataset>`

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

**right-dataset**
- Required
- Description: Right dataset could be either an index or a subquery with/without alias.

### Example 1: two indices join

PPL query:

    os> source=customer | join ON c_custkey = o_custkey orders
        | fields c_custkey, c_nationkey, c_mktsegment, o_orderkey, o_orderstatus, o_totalprice | head 10
    fetched rows / total rows = 10/10
    +----------+-------------+-------------+------------+---------------+-------------+
    | c_custkey| c_nationkey | c_mktsegment| o_orderkey | o_orderstatus | o_totalprice|
    +----------+-------------+-------------+------------+---------------+-------------+
    | 36901    | 13          | AUTOMOBILE  | 1          | O             | 173665.47   |
    | 78002    | 10          | AUTOMOBILE  | 2          | O             | 46929.18    |
    | 123314   | 15          | MACHINERY   | 3          | F             | 193846.25   |
    | 136777   | 10          | HOUSEHOLD   | 4          | O             | 32151.78    |
    | 44485    | 20          | FURNITURE   | 5          | F             | 144659.2    |
    | 55624    | 7           | AUTOMOBILE  | 6          | F             | 58749.59    |
    | 39136    | 5           | FURNITURE   | 7          | O             | 252004.18   |
    | 130057   | 9           | FURNITURE   | 32         | O             | 208660.75   |
    | 66958    | 18          | MACHINERY   | 33         | F             | 163243.98   |
    | 61001    | 3           | FURNITURE   | 34         | O             | 58949.67    |
    +----------+-------------+-------------+------------+---------------+-------------+

### Example 2: three indices join

PPL query:

    os> source=customer | join ON c_custkey = o_custkey orders | join ON c_nationkey = n_nationkey nation
        | fields c_custkey, c_mktsegment, o_orderkey, o_orderstatus, o_totalprice, n_name | head 10
    fetched rows / total rows = 10/10
    +----------+-------------+------------+---------------+-------------+--------------+
    | c_custkey| c_mktsegment| o_orderkey | o_orderstatus | o_totalprice| n_name       |
    +----------+-------------+------------+---------------+-------------+--------------+
    | 36901    | AUTOMOBILE  | 1          | O             | 173665.47   | JORDAN       |
    | 78002    | AUTOMOBILE  | 2          | O             | 46929.18    | IRAN         |
    | 123314   | MACHINERY   | 3          | F             | 193846.25   | MOROCCO      |
    | 136777   | HOUSEHOLD   | 4          | O             | 32151.78    | IRAN         |
    | 44485    | FURNITURE   | 5          | F             | 144659.2    | SAUDI ARABIA |
    | 55624    | AUTOMOBILE  | 6          | F             | 58749.59    | GERMANY      |
    | 39136    | FURNITURE   | 7          | O             | 252004.18   | ETHIOPIA     |
    | 130057   | FURNITURE   | 32         | O             | 208660.75   | INDONESIA    |
    | 66958    | MACHINERY   | 33         | F             | 163243.98   | CHINA        |
    | 61001    | FURNITURE   | 34         | O             | 58949.67    | CANADA       |
    +----------+-------------+------------+---------------+-------------+--------------+

### Example 3: join a subquery in right side

PPL query:

    os>source=supplier| join right = revenue0 ON s_suppkey = supplier_no
         [
           source=lineitem | where l_shipdate >= date('1996-01-01') AND l_shipdate < date_add(date('1996-01-01'), interval 3 month)
           | eval supplier_no = l_suppkey | stats sum(l_extendedprice * (1 - l_discount)) as total_revenue by supplier_no
         ]
       | fields s_name, s_phone, total_revenue, supplier_no | head 10
    fetched rows / total rows = 10/10
    +---------------------+----------------+-------------------+-------------+
    | s_name              | s_phone        | total_revenue     | supplier_no |
    +---------------------+----------------+-------------------+-------------+
    | Supplier#000007747  | 24-911-546-3505| 636204.0279       | 7747        |
    | Supplier#000007748  | 29-535-184-2277| 538311.8099       | 7748        |
    | Supplier#000007749  | 18-225-478-7489| 743462.4473000001 | 7749        |
    | Supplier#000007750  | 28-680-484-7044| 616828.2220999999 | 7750        |
    | Supplier#000007751  | 20-990-606-7343| 1092975.1925      | 7751        |
    | Supplier#000007752  | 12-936-258-6650| 1090399.9666      | 7752        |
    | Supplier#000007753  | 22-394-329-1153| 777130.7457000001 | 7753        |
    | Supplier#000007754  | 26-941-591-5320| 866600.0501       | 7754        |
    | Supplier#000007755  | 32-138-467-4225| 702256.7030000001 | 7755        |
    | Supplier#000007756  | 29-860-205-8019| 1304979.0511999999| 7756        |
    +---------------------+----------------+-------------------+-------------+

### Example 4: complex example in OTEL

**Schema**

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

**Requirement**

For each service, join span index on service map index to calculate metrics under different type of filters.

![image](https://user-images.githubusercontent.com/28062824/194170062-f0dd1d57-c5eb-44db-95e0-6b3b4e52f25a.png)

This sample query calculates latency when filtered by trace group `client_cancel_order` for the `order` service. I only have a subquery example, don't have the join version of the query.

PPL query:
```
source=otel-v1-apm-span-000001
| WHERE serviceName = 'order'
| JOIN left=t1 right=t2
    ON t1.traceId = t2.traceId AND t2.serviceName = 'order'
    otel-v1-apm-span-000001 // self inner join
| RENAME s_name as t1.name
| RENAME s_parentSpanId as t1.parentSpanId
| RENAME s_durationInNanos as t1.durationInNanos 
| FIELDS s_name, s_parentSpanId, s_durationInNanos // reduce colunms in join
| LEFT JOIN left=s1 right=t3
    ON s_name = t3.target.resource AND t3.serviceName = 'order' AND t3.traceGroupName = 'client_cancel_order'
    otel-v1-apm-service-map
| WHERE (s_parentSpanId IS NOT NULL OR (s_parentSpanId IS NULL AND s_name = 'client_cancel_order'))
| STATS avg(s_durationInNanos)
```

### Comparison with [Correlation](ppl-correlation-command)

A primary difference between `correlate` and `join` is that both sides of `correlate` are tables, but both sides of `join` are subqueries. 
For example:
```
source = testTable1
| where country = 'Canada' OR country = 'England'
| eval cname = lower(name)
| fields cname, country, year, month
| inner join left=l right=r
    ON l.cname = r.name AND l.country = r.country AND l.year = 2023 AND r.month = 4
    testTable2s
```
The subquery alias `l` does not represent the `testTable1` table itself. Instead, it represents the subquery:
```
source = testTable1
| where country = 'Canada' OR country = 'England'
| eval cname = lower(name)
| fields cname, country, year, month
```
Therefore, the condition of `join` must be `subqueryAlias.field = subqueryAlias.field`, rather than `tableName.field = tableName.field` as in `correlate`.
