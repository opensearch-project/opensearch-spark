## Example PPL Queries

#### **Describe**
- `describe table`  This command is equal to the `DESCRIBE EXTENDED table` SQL command
- `describe schema.table`
- `` describe schema.`table` ``
- `describe catalog.schema.table`
- `` describe catalog.schema.`table` ``
- `` describe `catalog`.`schema`.`table` ``

#### **Explain**
- `explain simple | source = table | where a = 1 | fields a,b,c`
- `explain extended | source = table`
- `explain codegen | source = table | dedup a | fields a,b,c`
- `explain cost | source = table | sort a | fields a,b,c`
- `explain formatted | source = table | fields - a`
- `explain simple | describe table`

#### **Fields**
[See additional command details](commands/ppl-fields-command.md)
- `source = table`
- `source = table | fields a,b,c`
- `source = table | fields + a,b,c`
- `source = table | fields - b,c`
- `source = table | eval b1 = b | fields - b1,c`

_- **Limitation: new field added by eval command with a function cannot be dropped in current version:**_
- `source = table | eval b1 = b + 1 | fields - b1,c` (Field `b1` cannot be dropped caused by SPARK-49782)
- `source = table | eval b1 = lower(b) | fields - b1,c` (Field `b1` cannot be dropped caused by SPARK-49782)

**Nested-Fields**
- `source = catalog.schema.table1, catalog.schema.table2 | fields A.nested1, B.nested1`
- `source = catalog.table | where struct_col2.field1.subfield > 'valueA' | sort int_col | fields  int_col, struct_col.field1.subfield, struct_col2.field1.subfield`
- `source = catalog.schema.table | where struct_col2.field1.subfield > 'valueA' | sort int_col | fields  int_col, struct_col.field1.subfield, struct_col2.field1.subfield`

#### **Filters**
- `source = table | where a = 1 | fields a,b,c`
- `source = table | where a >= 1 | fields a,b,c`
- `source = table | where a < 1 | fields a,b,c`
- `source = table | where b != 'test' | fields a,b,c`
- `source = table | where c = 'test' | fields a,b,c | head 3`
- `source = table | where ispresent(b)`
- `source = table | where isnull(coalesce(a, b)) | fields a,b,c | head 3`
- `source = table | where isempty(a)`
- `source = table | where isblank(a)`
- `source = table | where case(length(a) > 6, 'True' else 'False') = 'True'`

```sql
 source = table | eval status_category =
 case(a >= 200 AND a < 300, 'Success',
         a >= 300 AND a < 400, 'Redirection',
         a >= 400 AND a < 500, 'Client Error',
         a >= 500, 'Server Error'
     else 'Incorrect HTTP status code')
 | where case(a >= 200 AND a < 300, 'Success',
         a >= 300 AND a < 400, 'Redirection',
         a >= 400 AND a < 500, 'Client Error',
         a >= 500, 'Server Error'
    else 'Incorrect HTTP status code'
 ) = 'Incorrect HTTP status code'
```


```sql
 source = table
     | eval factor = case(a > 15, a - 14, isnull(b), a - 7, a < 3, a + 1 else 1)
     | where case(factor = 2, 'even', factor = 4, 'even', factor = 6, 'even', factor = 8, 'even' else 'odd') = 'even'
     | stats count() by factor
 ```

#### **Filters With Logical Conditions**
- `source = table | where c = 'test' AND a = 1 | fields a,b,c`
- `source = table | where c != 'test' OR a > 1 | fields a,b,c | head 1`
- `source = table | where c = 'test' NOT a > 1 | fields a,b,c`


#### **Eval**:
[See additional command details](commands/ppl-eval-command.md)

Assumptions: `a`, `b`, `c` are existing fields in `table`
- `source = table | eval f = 1 | fields a,b,c,f`
- `source = table | eval f = 1` (output a,b,c,f fields)
- `source = table | eval n = now() | eval t = unix_timestamp(a) | fields n,t`
- `source = table | eval f = a | where f > 1 | sort f | fields a,b,c | head 5`
- `source = table | eval f = a * 2 | eval h = f * 2 | fields a,f,h`
- `source = table | eval f = a * 2, h = f * 2 | fields a,f,h`
- `source = table | eval f = a * 2, h = b | stats avg(f) by h`
- `source = table | eval f = ispresent(a)`
- `source = table | eval r = coalesce(a, b, c) | fields r`
- `source = table | eval e = isempty(a) | fields e`
- `source = table | eval e = isblank(a) | fields e`
- `source = table | eval f = case(a = 0, 'zero', a = 1, 'one', a = 2, 'two', a = 3, 'three', a = 4, 'four', a = 5, 'five', a = 6, 'six', a = 7, 'se7en', a = 8, 'eight', a = 9, 'nine')`
- `source = table | eval f = case(a = 0, 'zero', a = 1, 'one' else 'unknown')`
- `source = table | eval f = case(a = 0, 'zero', a = 1, 'one' else concat(a, ' is an incorrect binary digit'))`

#### Fillnull
Assumptions: `a`, `b`, `c`, `d`, `e` are existing fields in `table`
- `source = table | fillnull with 0 in a`
- `source = table | fillnull with 'N/A' in a, b, c`
- `source = table | fillnull with concat(a, b) in c, d`
- `source = table | fillnull using a = 101`
- `source = table | fillnull using a = 101, b = 102`
- `source = table | fillnull using a = concat(b, c), d = 2 * pi() * e`

```sql
source = table | eval e = eval status_category =
        case(a >= 200 AND a < 300, 'Success',
            a >= 300 AND a < 400, 'Redirection',
            a >= 400 AND a < 500, 'Client Error',
            a >= 500, 'Server Error'
        else 'Unknown'
    )
```

```sql
source = table |  where ispresent(a) |
    eval status_category =
         case(a >= 200 AND a < 300, 'Success',
          a >= 300 AND a < 400, 'Redirection',
          a >= 400 AND a < 500, 'Client Error',
          a >= 500, 'Server Error'
          else 'Incorrect HTTP status code'
         )
    | stats count() by status_category
```

**Limitation**:
    Overriding existing field is unsupported, following queries throw exceptions with "Reference 'a' is ambiguous"

- `source = table | eval a = 10 | fields a,b,c`
- `source = table | eval a = a * 2 | stats avg(a)`
- `source = table | eval a = abs(a) | where a > 0`
- `source = table | eval a = signum(a) | where a < 0`

#### **Aggregations**
[See additional command details](commands/ppl-stats-command.md)

- `source = table | stats avg(a) `
- `source = table | where a < 50 | stats avg(c) `
- `source = table | stats max(c) by b`
- `source = table | stats count(c) by b | head 5`
- `source = table | stats distinct_count(c)`
- `source = table | stats stddev_samp(c)`
- `source = table | stats stddev_pop(c)`
- `source = table | stats percentile(c, 90)`
- `source = table | stats percentile_approx(c, 99)`

**Aggregations With Span**
- `source = table  | stats count(a) by span(a, 10) as a_span`
- `source = table  | stats sum(age) by span(age, 5) as age_span | head 2`
- `source = table  | stats avg(age) by span(age, 20) as age_span, country  | sort - age_span |  head 2`

**Aggregations With TimeWindow Span (tumble windowing function)**
- `source = table | stats sum(productsAmount) by span(transactionDate, 1d) as age_date | sort age_date`
- `source = table | stats sum(productsAmount) by span(transactionDate, 1w) as age_date, productId`

**Aggregations Group by Multiple Levels**
- `source = table | stats avg(age) as avg_state_age by country, state | stats avg(avg_state_age) as avg_country_age by country`
- `source = table | stats avg(age) as avg_city_age by country, state, city | eval new_avg_city_age = avg_city_age - 1 | stats avg(new_avg_city_age) as avg_state_age by country, state | where avg_state_age > 18 | stats avg(avg_state_age) as avg_adult_country_age by country`

#### **Dedup**

[See additional command details](commands/ppl-dedup-command.md)

- `source = table | dedup a | fields a,b,c`
- `source = table | dedup a,b | fields a,b,c`
- `source = table | dedup a keepempty=true | fields a,b,c`
- `source = table | dedup a,b keepempty=true | fields a,b,c`
- `source = table | dedup 1 a | fields a,b,c`
- `source = table | dedup 1 a,b | fields a,b,c`
- `source = table | dedup 1 a keepempty=true | fields a,b,c`
- `source = table | dedup 1 a,b keepempty=true | fields a,b,c`
- `source = table | dedup 2 a | fields a,b,c`
- `source = table | dedup 2 a,b | fields a,b,c`
- `source = table | dedup 2 a keepempty=true | fields a,b,c`
- `source = table | dedup 2 a,b keepempty=true | fields a,b,c`
- `source = table | dedup 1 a consecutive=true| fields a,b,c` (Consecutive deduplication is unsupported)

#### **Rare**
[See additional command details](commands/ppl-rare-command.md)

- `source=accounts | rare gender`
- `source=accounts | rare age by gender`

#### **Top**
[See additional command details](commands/ppl-top-command.md)

- `source=accounts | top gender`
- `source=accounts | top 1 gender`
- `source=accounts | top 1 age by gender`

#### **Parse**
[See additional command details](commands/ppl-parse-command.md)

- `source=accounts | parse email '.+@(?<host>.+)' | fields email, host `
- `source=accounts | parse email '.+@(?<host>.+)' | top 1 host `
- `source=accounts | parse email '.+@(?<host>.+)' | stats count() by host`
- `source=accounts | parse email '.+@(?<host>.+)' | eval eval_result=1 | fields host, eval_result`
- `source=accounts | parse email '.+@(?<host>.+)' | where age > 45 | sort - age | fields age, email, host`
- `source=accounts | parse address '(?<streetNumber>\d+) (?<street>.+)' | where streetNumber > 500 | sort num(streetNumber) | fields streetNumber, street`
- Limitation: [see limitations](commands/ppl-parse-command.md#limitations)

#### **Grok**
[See additional command details](commands/ppl-grok-command.md)

- `source=accounts | grok email '.+@%{HOSTNAME:host}' | top 1 host`
- `source=accounts | grok email '.+@%{HOSTNAME:host}' | stats count() by host`
- `source=accounts | grok email '.+@%{HOSTNAME:host}' | eval eval_result=1 | fields host, eval_result`
- `source=accounts | grok email '.+@%{HOSTNAME:host}' | eval eval_result=1 | fields host, eval_result`
- `source=accounts | grok street_address '%{NUMBER} %{GREEDYDATA:address}' | fields address `
- `source=logs | grok message '%{COMMONAPACHELOG}' | fields COMMONAPACHELOG, timestamp, response, bytes`

- **Limitation: Overriding existing field is unsupported:**_
- `source=accounts | grok address '%{NUMBER} %{GREEDYDATA:address}' | fields address`
- [see limitations](commands/ppl-parse-command.md#limitations)

#### **Patterns**
[See additional command details](commands/ppl-patterns-command.md)

- `source=accounts | patterns email | fields email, patterns_field `
- `source=accounts | patterns email | where age > 45 | sort - age | fields email, patterns_field`
- `source=apache | patterns new_field='no_numbers' pattern='[0-9]' message | fields message, no_numbers`
- `source=apache | patterns new_field='no_numbers' pattern='[0-9]' message | stats count() by no_numbers`
- Limitation: [see limitations](commands/ppl-parse-command.md#limitations)

#### **Rename**
[See additional command details](commands/ppl-rename-command.md)

- `source=accounts | rename email as user_email | fields id, user_email`
- `source=accounts | rename id as user_id, email as user_email | fields user_id, user_email`


#### **Join**
[See additional command details](commands/ppl-join-command.md)

- `source = table1 | inner join left = l right = r on l.a = r.a table2 | fields l.a, r.a, b, c`
- `source = table1 | left join left = l right = r on l.a = r.a table2 | fields l.a, r.a, b, c`
- `source = table1 | right join left = l right = r on l.a = r.a table2 | fields l.a, r.a, b, c`
- `source = table1 | full left = l right = r on l.a = r.a table2 | fields l.a, r.a, b, c`
- `source = table1 | cross join left = l right = r table2`
- `source = table1 | left semi join left = l right = r on l.a = r.a table2`
- `source = table1 | left anti join left = l right = r on l.a = r.a table2`

_- **Limitation: sub-searches is unsupported in join right side now**_


#### **Lookup**
[See additional command details](commands/ppl-lookup-command.md)

- `source = table1 | lookup table2 id`
- `source = table1 | lookup table2 id, name`
- `source = table1 | lookup table2 id as cid, name`
- `source = table1 | lookup table2 id as cid, name replace dept as department`
- `source = table1 | lookup table2 id as cid, name replace dept as department, city as location`
- `source = table1 | lookup table2 id as cid, name append dept as department`
- `source = table1 | lookup table2 id as cid, name append dept as department, city as location`
- `source = table1 | lookup table2 id as cid, name replace dept` (dept without "as" is unsupported)

_- **Limitation: "REPLACE" or "APPEND" clause must contain "AS"**_


#### **InSubquery**
[See additional command details](commands/ppl-subquery-command.md)

- `source = outer | where a in [ source = inner | fields b ]`
- `source = outer | where (a) in [ source = inner | fields b ]`
- `source = outer | where (a,b,c) in [ source = inner | fields d,e,f ]`
- `source = outer | where a not in [ source = inner | fields b ]`
- `source = outer | where (a) not in [ source = inner | fields b ]`
- `source = outer | where (a,b,c) not in [ source = inner | fields d,e,f ]`
- `source = outer | where a in [ source = inner1 | where b not in [ source = inner2 | fields c ] | fields b ]` (nested)
- `source = table1 | inner join left = l right = r on l.a = r.a AND r.a in [ source = inner | fields d ] | fields l.a, r.a, b, c` (as join filter)

**SQL Migration examples with IN-Subquery PPL:**

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
#### **ScalarSubquery**
[See additional command details](ppl-subquery-command.md)

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


---
#### Experimental Commands:
[See additional command details](commands/ppl-correlation-command.md)

```sql
- `source alb_logs, traces, metrics | where ip="10.0.0.1" AND cloud.provider="aws"| correlate exact on (ip, port) scope(@timestamp, 2018-07-02T22:23:00, 1 D)`
- `source alb_logs, traces | where alb_logs.ip="10.0.0.1" AND alb_logs.cloud.provider="aws"|
    correlate exact fields(traceId, ip) scope(@timestamp, 1D) mapping(alb_logs.ip = traces.attributes.http.server.address, alb_logs.traceId = traces.traceId ) `
```

> ppl-correlation-command is an experimental command - it may be removed in future versions

---
### Planned Commands:

#### **fillnull**

```sql
   -  `source=accounts | fillnull fields status_code=101`
   -  `source=accounts | fillnull fields request_path='/not_found', timestamp='*'`
    - `source=accounts | fillnull using field1=101`
    - `source=accounts | fillnull using field1=concat(field2, field3), field4=2*pi()*field5`
    - `source=accounts | fillnull using field1=concat(field2, field3), field4=2*pi()*field5, field6 = 'N/A'`
```
[See additional command details](planning/ppl-fillnull-command.md)
