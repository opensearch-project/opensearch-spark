## PPL SubQuery Commands

### Description
The subquery command has 4 types: `InSubquery`, `ExistsSubquery`, `ScalarSubquery` and `RelationSubquery`.
`InSubquery`, `ExistsSubquery` and `ScalarSubquery` are subquery expressions, their common usage is in Where clause(`where <boolean expression>`) and Search filter(`search source=* <boolean expression>`).

For example, a subquery expression could be used in boolean expression:
```
| where orders.order_id in [ source=returns | where return_reason="damaged" | field order_id ]
```
The `orders.order_id in [ source=... ]` is a `<boolean expression>`.

But `RelationSubquery` is not a subquery expression, it is a subquery plan.
[Recall the join command doc](ppl-join-command.md), the example is a subquery/subsearch **plan**, rather than a **expression**.

### Syntax
- `where <field> [not] in [ source=... | ... | ... ]` (InSubquery)
- `where [not] exists [ source=... | ... | ... ]` (ExistsSubquery)
- `where <field> = [ source=... | ... | ... ]` (ScalarSubquery)
- `source=[ source= ...]` (RelationSubquery)
- `| join ON condition [ source= ]` (RelationSubquery in join right side)

### Usage
InSubquery:
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

ExistsSubquery:

(Assumptions: `a`, `b` are fields of table outer, `c`, `d` are fields of table inner,  `e`, `f` are fields of table inner2)
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

ScalarSubquery:

(Assumptions: `a`, `b` are fields of table outer, `c`, `d` are fields of table inner,  `e`, `f` are fields of table nested)

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

RelationSubquery:
- `source = table1 | join left = l right = r on condition [ source = table2 | where d > 10 | head 5 ]` (subquery in join right side)
- `source = [ source = table1 | join left = l right = r [ source = table2 | where d > 10 | head 5 ] | stats count(a) by b ] as outer | head 1`

### Examples 1: TPC-H q20

PPL query:

    os> source=supplier
        | join ON s_nationkey = n_nationkey nation
        | where n_name = 'CANADA'
            and s_suppkey in [  // InSubquery
                source = partsupp
                | where ps_partkey in [ // InSubquery
                    source = part
                    | where like(p_name, 'forest%')
                    | fields p_partkey
                ]
                and ps_availqty > [ // ScalarSubquery
                    source = lineitem
                    | where l_partkey = ps_partkey
                        and l_suppkey = ps_suppkey
                        and l_shipdate >= date('1994-01-01')
                        and l_shipdate < date_add(date('1994-01-01'), interval 1 year)
                    | stats sum(l_quantity) as sum_l_quantity
                    | eval half_sum_l_quantity = 0.5 * sum_l_quantity // Stats and Eval commands can combine when issues/819 resolved
                    | fields half_sum_l_quantity
                ]
            | fields ps_suppkey
        ]
        | fields s_suppkey, s_name, s_phone, s_acctbal, n_name | head 10
    fetched rows / total rows = 10/10
    +-----------+---------------------+----------------+----------+---------+
    | s_suppkey | s_name              | s_phone        | s_acctbal| n_name  |
    +-----------+---------------------+----------------+----------+---------+
    | 8243      | Supplier#000008243  | 13-707-547-1386| 9067.07  | CANADA  |
    | 736       | Supplier#000000736  | 13-681-806-8650| 5700.83  | CANADA  |
    | 9032      | Supplier#000009032  | 13-441-662-5539| 3982.32  | CANADA  |
    | 3201      | Supplier#000003201  | 13-600-413-7165| 3799.41  | CANADA  |
    | 3849      | Supplier#000003849  | 13-582-965-9117| 52.33    | CANADA  |
    | 5505      | Supplier#000005505  | 13-531-190-6523| 2023.4   | CANADA  |
    | 5195      | Supplier#000005195  | 13-622-661-2956| 3717.34  | CANADA  |
    | 9753      | Supplier#000009753  | 13-724-256-7877| 4406.93  | CANADA  |
    | 7135      | Supplier#000007135  | 13-367-994-6705| 4950.29  | CANADA  |
    | 5256      | Supplier#000005256  | 13-180-538-8836| 5624.79  | CANADA  |
    +-----------+---------------------+----------------+----------+---------+


### Examples 2: TPC-H q22

PPL query:

    os> source = [
            source = customer
            | where substring(c_phone, 1, 2) in ('13', '31', '23', '29', '30', '18', '17')
            and c_acctbal > [
                source = customer
                | where c_acctbal > 0.00
                    and substring(c_phone, 1, 2) in ('13', '31', '23', '29', '30', '18', '17')
                | stats avg(c_acctbal)
            ]
            and not exists [
                source = orders
                | where o_custkey = c_custkey
            ]
            | eval cntrycode = substring(c_phone, 1, 2)
            | fields cntrycode, c_acctbal
        ] as custsale
        | stats count() as numcust, sum(c_acctbal) as totacctbal by cntrycode
        | sort cntrycode
    fetched rows / total rows = 10/10
    +---------+--------------------+------------+
    | numcust | totacctbal         | cntrycode  |
    +---------+--------------------+------------+
    | 888     | 6737713.989999999  | 13         |
    | 861     | 6460573.72         | 17         |
    | 964     | 7236687.4          | 18         |
    | 892     | 6701457.950000001  | 23         |
    | 948     | 7158866.630000001  | 29         |
    | 909     | 6808436.129999999  | 30         |
    | 922     | 6806670.179999999  | 31         |
    +---------+--------------------+------------+

### Additional Context

#### RelationSubquery

RelationSubquery is plan instead of expression, for example
```
source=customer
| FIELDS c_custkey
| LEFT OUTER JOIN left = c right = o ON c.c_custkey = o.o_custkey
   [
      SEARCH source=orders
      | WHERE o_comment NOT LIKE '%unusual%packages%'
      | FIELDS o_orderkey, o_custkey
   ]
| STATS ...
```
simply into
```
SEARCH <leftPlan>
| LEFT OUTER JOIN ON <condition>
   [
      <rightPlan>
   ]
| STATS ...
```

#### Uncorrelated Subquery

An uncorrelated subquery is independent of the outer query. It is executed once, and the result is used by the outer query.
It's **less common** when using `ExistsSubquery` because `ExistsSubquery` typically checks for the presence of rows that are dependent on the outer query’s row.

There is a very special exists subquery which highlight by `(special uncorrelated exists)`:
```
SELECT 'nonEmpty'
FROM outer
    WHERE EXISTS (
        SELECT *
        FROM inner
    );
```
Rewritten by PPL ExistsSubquery query:
```
source = outer
| where exists [
    source = inner
  ]
| eval l = "nonEmpty"
| fields l
```
This query just print "nonEmpty" if the inner table is not empty.

#### Table alias in subquery

Table alias is useful in query which contains a subquery, for example

```
select a, (
             select sum(b)
             from catalog.schema.table1 as t1
             where t1.a = t2.a
          )  sum_b
 from catalog.schema.table2 as t2
```
`t1` and `t2` are table aliases which are used in correlated subquery, `sum_b` are subquery alias.
