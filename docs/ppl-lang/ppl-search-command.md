## PPL `search` command

### Description
Using ``search`` command to retrieve document from the index. ``search`` command could be only used as the first command in the PPL query.


### Syntax
`search source=[<remote-cluster>:]<index> [boolean-expression]`

* search: search keywords, which could be ignore.
* index: mandatory. search command must specify which index to query from. The index name can be prefixed by "<cluster name>:" for cross-cluster search.
* bool-expression: optional. any expression which could be evaluated to boolean value.


### Example 1: Fetch all the data
The example show fetch all the document from accounts index.

PPL query:

    os> source=accounts;
    +------------------+-------------+----------------------+-----------+----------+--------+------------+---------+-------+-----------------------+------------+
    | account_number   | firstname   | address              | balance   | gender   | city   | employer   | state   | age   | email                 | lastname   |
    |------------------+-------------+----------------------+-----------+----------+--------+------------+---------+-------+-----------------------+------------|
    | 1                | Amber       | 880 Holmes Lane      | 39225     | M        | Brogan | Pyrami     | IL      | 32    | amberduke@pyrami.com  | Duke       |
    | 6                | Hattie      | 671 Bristol Street   | 5686      | M        | Dante  | Netagy     | TN      | 36    | hattiebond@netagy.com | Bond       |
    | 13               | Nanette     | 789 Madison Street   | 32838     | F        | Nogal  | Quility    | VA      | 28    | null                  | Bates      |
    | 18               | Dale        | 467 Hutchinson Court | 4180      | M        | Orick  | null       | MD      | 33    | daleadams@boink.com   | Adams      |
    +------------------+-------------+----------------------+-----------+----------+--------+------------+---------+-------+-----------------------+------------+

### Example 2: Fetch data with condition
The example show fetch all the document from accounts index with .

PPL query:

    os> SEARCH source=accounts account_number=1 or gender="F";
    +------------------+-------------+--------------------+-----------+----------+--------+------------+---------+-------+----------------------+------------+
    | account_number   | firstname   | address            | balance   | gender   | city   | employer   | state   | age   | email                | lastname   |
    |------------------+-------------+--------------------+-----------+----------+--------+------------+---------+-------+----------------------+------------|
    | 1                | Amber       | 880 Holmes Lane    | 39225     | M        | Brogan | Pyrami     | IL      | 32    | amberduke@pyrami.com | Duke       |
    | 13               | Nanette     | 789 Madison Street | 32838     | F        | Nogal  | Quility    | VA      | 28    | null                 | Bates      |
    +------------------+-------------+--------------------+-----------+----------+--------+------------+---------+-------+----------------------+------------+

### Example 3: Fetch data with a sampling percentage ( including an aggregation)
The following example demonstrates how to sample 50% of the data from the table and then perform aggregation (finding rare occurrences of address).

PPL query:

    os> source = account  sample(75 percent) | top 3 country by occupation

This query samples 75% of the records from account table, then retrieves the top 3 countries grouped by occupation

```sql
SELECT *
FROM (
         SELECT country, occupation, COUNT(country) AS count_country
         FROM account
                  TABLESAMPLE(75 PERCENT)
         GROUP BY country, occupation
         ORDER BY COUNT(country) DESC NULLS LAST
             LIMIT 3
     ) AS subquery
    LIMIT 3;
```
Logical Plan Equivalent:

```sql
'Project [*]
+- 'GlobalLimit 3
   +- 'LocalLimit 3
      +- 'Sort ['COUNT('country) AS count_country#68 DESC NULLS LAST], true
         +- 'Aggregate ['country, 'occupation AS occupation#67], ['COUNT('country) AS count_country#66, 'country, 'occupation AS occupation#67]
            +- 'Sample 0.0, 0.75, false, 0
               +- 'UnresolvedRelation [account], [], false

```

By introducing the `sample` instruction into the source command, one can now sample data as part of your queries and reducing the amount of data being scanned thereby converting precision with performance.

The `percent` parameter will give the actual approximation of the true value with the needed trade of between accuracy and performance.