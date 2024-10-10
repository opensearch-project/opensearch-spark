## PPL SubQuery Commands:

**Syntax**
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

**InSubquery usage**
- `source = outer | where a in [ source = inner | fields b ]`
- `source = outer | where (a) in [ source = inner | fields b ]`
- `source = outer | where (a,b,c) in [ source = inner | fields d,e,f ]`
- `source = outer | where a not in [ source = inner | fields b ]`
- `source = outer | where (a) not in [ source = inner | fields b ]`
- `source = outer | where (a,b,c) not in [ source = inner | fields d,e,f ]`
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

**ScalarSubquery usage**

Assumptions: `a`, `b` are fields of table outer, `c`, `d` are fields of table inner,  `e`, `f` are fields of table nested

**Uncorrelated scalar subquery in Select**
- `source = outer | eval m = [ source = inner | stats max(c) ] | fields m, a`
- `source = outer | eval m = [ source = inner | stats max(c) ] + b | fields m, a`

**Uncorrelated scalar subquery in Select and Where**
- `source = outer | where a > [ source = inner | stats min(c) ] | eval m = [ source = inner | stats max(c) ] | fields m, a`

**Correlated scalar subquery in Select**
- `source = outer | eval m = [ source = inner | where outer.b = inner.d | stats max(c) ] | fields m, a`
- `source = outer | eval m = [ source = inner | where b = d | stats max(c) ] | fields m, a`
- `source = outer | eval m = [ source = inner | where outer.b > inner.d | stats max(c) ] | fields m, a`

**Correlated scalar subquery in Where**
- `source = outer | where a = [ source = inner | where outer.b = inner.d | stats max(c) ]`
- `source = outer | where a = [ source = inner | where b = d | stats max(c) ]`
- `source = outer | where [ source = inner | where outer.b = inner.d OR inner.d = 1 | stats count() ] > 0 | fields a`

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

### **Additional Context**

The most cases in the description is to request a `InSubquery` expression.

The `where` command syntax is:

```
| where <boolean expression>
```
So the subquery in description is part of boolean expression, such as

```sql
| where orders.order_id in (subquery source=returns | where return_reason="damaged" | return order_id)
```

The `orders.order_id in (subquery source=...)` is a `<boolean expression>`.

In general, we name this kind of subquery clause the `InSubquery` expression, it is a `<boolean expression>`, one kind of `subquery expressions`.

PS: there are many kinds of `subquery expressions`, another commonly used one is `ScalarSubquery` expression:

**Subquery with Different Join Types**

In issue description is a `ScalarSubquery`:

```sql
source=employees
| join source=sales on employees.employee_id = sales.employee_id
| where sales.sale_amount > (subquery source=targets | where target_met="true" | return target_value)
```

Recall the join command doc: https://github.com/opensearch-project/opensearch-spark/blob/main/docs/PPL-Join-command.md#more-examples, the example is a subquery/subsearch **plan**, rather than a **expression**.

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
search <leftPlan> | left join on <condition> (subquery search ...)
```

The `(subquery search ...)` is not a `expression`, it's `plan`, similar to the `relation` plan