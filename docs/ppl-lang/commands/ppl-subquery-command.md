## PPL SubQuery Commands:

**Syntax**
The subquery command should be implemented using a clean, logical syntax that integrates with existing PPL structure.

```sql
source=logs | where field in (subquery source=events | where condition | return field)
```

In this example, the primary search (`source=logs`) is filtered by results from the subquery (`source=events`).

The subquery command should allow nested queries to be as complex as necessary, supporting multiple levels of nesting.

Example:

```sql
  source=logs | where field in (subquery source=users | where user in (subquery source=actions | where action="login"))
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