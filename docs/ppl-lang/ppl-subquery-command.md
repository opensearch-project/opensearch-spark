## PPL SubQuery Commands:

### Syntax
The subquery command should be implemented using a clean, logical syntax that integrates with existing PPL structure.

```sql
source=logs | where field in [ subquery source=events | where condition | fields field ]
```

In this example, the primary search (`source=logs`) is filtered by results from the subquery (`source=events`).

The subquery command should allow nested queries to be as complex as necessary, supporting multiple levels of nesting.

Example:

```sql
  source=logs | where id in [ subquery source=users | where user in [ subquery source=actions | where action="login" | fields user] | fields uid ]
```

For additional info See [Issue](https://github.com/opensearch-project/opensearch-spark/issues/661)

---

### InSubquery usage
- `source = outer | where a in [ source = inner | fields b ]`
- `source = outer | where (a) in [ source = inner | fields b ]`
- `source = outer | where (a,b,c) in [ source = inner | fields d,e,f ]`
- `source = outer | where a not in [ source = inner | fields b ]`
- `source = outer | where (a) not in [ source = inner | fields b ]`
- `source = outer | where (a,b,c) not in [ source = inner | fields d,e,f ]`
- `source = outer a in [ source = inner | fields b ]` (search filtering with subquery)
- `source = outer a not in [ source = inner | fields b ]` (search filtering with subquery)
- `source = outer | where a in [ source = inner1 | where b not in [ source = inner2 | fields c ] | fields b ]` (nested)
- `source = table1 | inner join left = l right = r on l.a = r.a AND r.a in [ source = inner | fields d ] | fields l.a, r.a, b, c` (as join filter)

**_SQL Migration examples with IN-Subquery PPL:_**
1. tpch q4 (in-subquery with aggregation)
```sql
select
  o_orderpriority,
  count(*) as order_count
from
  orders
where
  o_orderdate >= date '1993-07-01'
  and o_orderdate < date '1993-07-01' + interval '3' month
  and o_orderkey in (
    select
      l_orderkey
    from
      lineitem
    where l_commitdate < l_receiptdate
  )
group by
  o_orderpriority
order by
  o_orderpriority
```
Rewritten by PPL InSubquery query:
```sql
source = orders
| where o_orderdate >= "1993-07-01" and o_orderdate < "1993-10-01" and o_orderkey IN
  [ source = lineitem
    | where l_commitdate < l_receiptdate
    | fields l_orderkey
  ]
| stats count(1) as order_count by o_orderpriority
| sort o_orderpriority
| fields o_orderpriority, order_count
```
2.tpch q20 (nested in-subquery)
```sql
select
  s_name,
  s_address
from
  supplier,
  nation
where
  s_suppkey in (
    select
      ps_suppkey
    from
      partsupp
    where
      ps_partkey in (
        select
          p_partkey
        from
          part
        where
          p_name like 'forest%'
      )
  )
  and s_nationkey = n_nationkey
  and n_name = 'CANADA'
order by
  s_name
```
Rewritten by PPL InSubquery query:
```sql
source = supplier
| where s_suppkey IN [
    source = partsupp
    | where ps_partkey IN [
        source = part
        | where like(p_name, "forest%")
        | fields p_partkey
      ]
    | fields ps_suppkey
  ]
| inner join left=l right=r on s_nationkey = n_nationkey and n_name = 'CANADA'
  nation
| sort s_name
```
---

### ExistsSubquery usage

Assumptions: `a`, `b` are fields of table outer, `c`, `d` are fields of table inner,  `e`, `f` are fields of table inner2

- `source = outer | where exists [ source = inner | where a = c ]`
- `source = outer | where not exists [ source = inner | where a = c ]`
- `source = outer | where exists [ source = inner | where a = c and b = d ]`
- `source = outer | where not exists [ source = inner | where a = c and b = d ]`
- `source = outer exists [ source = inner | where a = c ]` (search filtering with subquery)
- `source = outer not exists [ source = inner | where a = c ]` (search filtering with subquery)
- `source = table as t1 exists [ source = table as t2 | where t1.a = t2.a ]` (table alias is useful in exists subquery)
- `source = outer | where exists [ source = inner1 | where a = c and exists [ source = inner2 | where c = e ] ]` (nested)
- `source = outer | where exists [ source = inner1 | where a = c | where exists [ source = inner2 | where c = e ] ]` (nested)
- `source = outer | where exists [ source = inner | where c > 10 ]` (uncorrelated exists)
- `source = outer | where not exists [ source = inner | where c > 10 ]` (uncorrelated exists)
- `source = outer | where exists [ source = inner ] | eval l = "nonEmpty" | fields l` (special uncorrelated exists)

**_SQL Migration examples with Exists-Subquery PPL:_**

tpch q4 (exists subquery with aggregation)
```sql
select
  o_orderpriority,
  count(*) as order_count
from
  orders
where
  o_orderdate >= date '1993-07-01'
  and o_orderdate < date '1993-07-01' + interval '3' month
  and exists (
    select
      l_orderkey
    from
      lineitem
    where l_orderkey = o_orderkey
      and l_commitdate < l_receiptdate
  )
group by
  o_orderpriority
order by
  o_orderpriority
```
Rewritten by PPL ExistsSubquery query:
```sql
source = orders
| where o_orderdate >= "1993-07-01" and o_orderdate < "1993-10-01"
    and exists [
      source = lineitem
      | where l_orderkey = o_orderkey and l_commitdate < l_receiptdate
    ]
| stats count(1) as order_count by o_orderpriority
| sort o_orderpriority
| fields o_orderpriority, order_count
```
---

### ScalarSubquery usage

Assumptions: `a`, `b` are fields of table outer, `c`, `d` are fields of table inner,  `e`, `f` are fields of table nested

**Uncorrelated scalar subquery in Select**
- `source = outer | eval m = [ source = inner | stats max(c) ] | fields m, a`
- `source = outer | eval m = [ source = inner | stats max(c) ] + b | fields m, a`

**Uncorrelated scalar subquery in Where**
- `source = outer | where a > [ source = inner | stats min(c) ] | fields a`

**Uncorrelated scalar subquery in Search filter**
- `source = outer a > [ source = inner | stats min(c) ] | fields a`

**Correlated scalar subquery in Select**
- `source = outer | eval m = [ source = inner | where outer.b = inner.d | stats max(c) ] | fields m, a`
- `source = outer | eval m = [ source = inner | where b = d | stats max(c) ] | fields m, a`
- `source = outer | eval m = [ source = inner | where outer.b > inner.d | stats max(c) ] | fields m, a`

**Correlated scalar subquery in Where**
- `source = outer | where a = [ source = inner | where outer.b = inner.d | stats max(c) ]`
- `source = outer | where a = [ source = inner | where b = d | stats max(c) ]`
- `source = outer | where [ source = inner | where outer.b = inner.d OR inner.d = 1 | stats count() ] > 0 | fields a`

**Correlated scalar subquery in Search filter**
- `source = outer a = [ source = inner | where b = d | stats max(c) ]`
- `source = outer [ source = inner | where outer.b = inner.d OR inner.d = 1 | stats count() ] > 0 | fields a`

**Nested scalar subquery**
- `source = outer | where a = [ source = inner | stats max(c) | sort c ] OR b = [ source = inner | where c = 1 | stats min(d) | sort d ]`
- `source = outer | where a = [ source = inner | where c =  [ source = nested | stats max(e) by f | sort f ] | stats max(d) by c | sort c | head 1 ]`

_SQL Migration examples with Scalar-Subquery PPL:_
Example 1
```sql
SELECT *
FROM   outer
WHERE  a = (SELECT   max(c)
            FROM     inner1
            WHERE c = (SELECT   max(e)
                       FROM     inner2
                       GROUP BY f
                       ORDER BY f
                       )
            GROUP BY c
            ORDER BY c
            LIMIT 1)
```
Rewritten by PPL ScalarSubquery query:
```sql
source = spark_catalog.default.outer
| where a = [
    source = spark_catalog.default.inner1
    | where c = [
        source = spark_catalog.default.inner2
        | stats max(e) by f
        | sort f
      ]
    | stats max(d) by c
    | sort c
    | head 1
  ]
```
Example 2
```sql
SELECT * FROM outer
WHERE  a = (SELECT max(c)
            FROM   inner
            ORDER BY c)
OR     b = (SELECT min(d)
            FROM   inner
            WHERE  c = 1
            ORDER BY d)
```
Rewritten by PPL ScalarSubquery query:
```sql
source = spark_catalog.default.outer
| where a = [
    source = spark_catalog.default.inner | stats max(c) | sort c
  ] OR b = [
    source = spark_catalog.default.inner | where c = 1 | stats min(d) | sort d
  ]
```
---

### (Relation) Subquery
`InSubquery`, `ExistsSubquery` and `ScalarSubquery` are all subquery expressions. But `RelationSubquery` is not a subquery expression, it is a subquery plan which is common used in Join or From clause.

- `source = table1 | join left = l right = r [ source = table2 | where d > 10 | head 5 ]` (subquery in join right side)
- `source = [ source = table1 | join left = l right = r [ source = table2 | where d > 10 | head 5 ] | stats count(a) by b ] as outer | head 1`

**_SQL Migration examples with Subquery PPL:_**

tpch q13
```sql
select
    c_count,
    count(*) as custdist
from
    (
        select
            c_custkey,
            count(o_orderkey) as c_count
        from
            customer left outer join orders on
                c_custkey = o_custkey
                and o_comment not like '%special%requests%'
        group by
            c_custkey
    ) as c_orders
group by
    c_count
order by
    custdist desc,
    c_count desc
```
Rewritten by PPL (Relation) Subquery:
```sql
SEARCH source = [
  SEARCH source = customer
  | LEFT OUTER JOIN left = c right = o ON c_custkey = o_custkey
    [
      SEARCH source = orders
      | WHERE not like(o_comment, '%special%requests%')
    ]
  | STATS COUNT(o_orderkey) AS c_count BY c_custkey
] AS c_orders
| STATS COUNT(o_orderkey) AS c_count BY c_custkey
| STATS COUNT(1) AS custdist BY c_count
| SORT - custdist, - c_count
```
---

### Additional Context

`InSubquery`, `ExistsSubquery` and `ScalarSubquery` as subquery expressions, their common usage is in `where` clause and `search filter`.

Where command:
```
| where <boolean expression> | ...
```
Search filter:
```
search source=* <boolean expression> | ...
```
A subquery expression could be used in boolean expression, for example

```sql
| where orders.order_id in [ source=returns | where return_reason="damaged" | field order_id ]
```

The `orders.order_id in [ source=... ]` is a `<boolean expression>`.

In general, we name this kind of subquery clause the `InSubquery` expression, it is a `<boolean expression>`.

**Subquery with Different Join Types**

In issue description is a `ScalarSubquery`:

```sql
source=employees
| join source=sales on employees.employee_id = sales.employee_id
| where sales.sale_amount > [ source=targets | where target_met="true" | fields target_value ]
```

But `RelationSubquery` is not a subquery expression, it is a subquery plan.
[Recall the join command doc](ppl-join-command.md), the example is a subquery/subsearch **plan**, rather than a **expression**.

```sql
SEARCH source=customer
| FIELDS c_custkey
| LEFT OUTER JOIN left = c, right = o ON c.c_custkey = o.o_custkey
   [
      SEARCH source=orders
      | WHERE o_comment NOT LIKE '%unusual%packages%'
      | FIELDS o_orderkey, o_custkey
   ]
| STATS ...
```
simply into
```sql
SEARCH <leftPlan>
| LEFT OUTER JOIN ON <condition>
   [
      <rightPlan>
   ]
| STATS ...
```
Apply the syntax here and simply into

```sql
search <leftPlan> | left join on <condition> [ search ... ]
```

The `[ search ...]` is not a `expression`, it's `plan`, similar to the `relation` plan

**Uncorrelated Subquery**

An uncorrelated subquery is independent of the outer query. It is executed once, and the result is used by the outer query.
It's **less common** when using `ExistsSubquery` because `ExistsSubquery` typically checks for the presence of rows that are dependent on the outer query’s row.

There is a very special exists subquery which highlight by `(special uncorrelated exists)`:
```sql
SELECT 'nonEmpty'
FROM outer
    WHERE EXISTS (
        SELECT *
        FROM inner
    );
```
Rewritten by PPL ExistsSubquery query:
```sql
source = outer
| where exists [
    source = inner
  ]
| eval l = "nonEmpty"
| fields l
```
This query just print "nonEmpty" if the inner table is not empty.

**Table alias in subquery**

Table alias is useful in query which contains a subquery, for example

```sql
select a, (
             select sum(b)
             from catalog.schema.table1 as t1
             where t1.a = t2.a
          )  sum_b
 from catalog.schema.table2 as t2
```
`t1` and `t2` are table aliases which are used in correlated subquery, `sum_b` are subquery alias.
