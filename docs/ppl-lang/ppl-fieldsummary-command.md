## PPL `fieldsummary` command

### Description
Using `fieldsummary` command to :
 - Calculate basic statistics for each field (count, distinct count, min, max, avg, stddev, mean )
 - Determine the data type of each field

### Syntax

`... | fieldsummary <field-list> (nulls=true/false)`

* command accepts any preceding pipe before the terminal `fieldsummary` command and will take them into account. 
* `includefields`: list of all the columns to be collected with statistics into a unified result set
* `nulls`: optional; if the true, include the null values in the aggregation calculations (replace null with zero for numeric values)

### Example 1: 

PPL query:

    os> source = t | where status_code != 200 | fieldsummary includefields= status_code nulls=true
    +------------------+-------------+------------+------------+------------+------------+------------+------------+----------------|
    | Fields            | COUNT       | COUNT_DISTINCT    |  MIN  |  MAX   |  AVG   |  MEAN   |        STDDEV       | NUlls | TYPEOF |
    |------------------+-------------+------------+------------+------------+------------+------------+------------+----------------|
    | "status_code"    |      2      |         2         | 301   |   403  |  352.0 |  352.0  |  72.12489168102785  |  0    | "int"  |
    +------------------+-------------+------------+------------+------------+------------+------------+------------+----------------|

### Example 2: 

PPL query:

    os> source = t | fieldsummary includefields= id, status_code, request_path nulls=true
    +------------------+-------------+------------+------------+------------+------------+------------+------------+----------------|
    | Fields            | COUNT       | COUNT_DISTINCT    |  MIN  |  MAX   |  AVG   |  MEAN   |        STDDEV       | NUlls | TYPEOF |
    |------------------+-------------+------------+------------+------------+------------+------------+------------+----------------|
    |       "id"       |      6      |         6         | 1     |   6    |  3.5   |   3.5  |  1.8708286933869707  |  0    | "int"  |
    +------------------+-------------+------------+------------+------------+------------+------------+------------+----------------|
    | "status_code"    |      4      |         3         | 200   |   403  |  184.0 |  184.0  |  161.16699413961905 |  2    | "int"  |
    +------------------+-------------+------------+------------+------------+------------+------------+------------+----------------|
    | "request_path"   |      2      |         2         | /about| /home  |  0.0    |  0.0     |      0              |  2    |"string"|
    +------------------+-------------+------------+------------+------------+------------+------------+------------+----------------|

### Additional Info
The actual query is translated into the following SQL-like statement:

```sql
     SELECT
         id AS Field,
         COUNT(id) AS COUNT,
        COUNT(DISTINCT id) AS COUNT_DISTINCT,
        MIN(id) AS MIN,
        MAX(id) AS MAX,
        AVG(id) AS AVG,
        MEAN(id) AS MEAN,
        STDDEV(id) AS STDDEV,
        (COUNT(1) - COUNT(id)) AS Nulls,
        TYPEOF(id) AS TYPEOF
     FROM
         t
     GROUP BY
         TYPEOF(status_code), status_code;
UNION
    SELECT
        status_code AS Field,
        COUNT(status_code) AS COUNT,
        COUNT(DISTINCT status_code) AS COUNT_DISTINCT,
        MIN(status_code) AS MIN,
        MAX(status_code) AS MAX,
        AVG(status_code) AS AVG,
        MEAN(status_code) AS MEAN,
        STDDEV(status_code) AS STDDEV,
        (COUNT(1) - COUNT(status_code)) AS Nulls,
        TYPEOF(status_code) AS TYPEOF
    FROM
        t
    GROUP BY
        TYPEOF(status_code), status_code;
```
For each such columns (id, status_code) there will be a unique statement and all the fields will be presented togather in the result using a UNION operator


### Limitation: 
 - `topvalues` option was removed from this command due the possible performance impact of such sub-query. As an alternative one can use the `top` command directly as shown [here](ppl-top-command.md). 

