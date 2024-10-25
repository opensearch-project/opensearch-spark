## PPL `eventstats` command

### Description
The `eventstats` command enriches your event data with calculated summary statistics. It operates by analyzing specified fields within your events, computing various statistical measures, and then appending these results as new fields to each original event.

Key aspects of `eventstats`:

1. It performs calculations across the entire result set or within defined groups.
2. The original events remain intact, with new fields added to contain the statistical results.
3. The command is particularly useful for comparative analysis, identifying outliers, or providing additional context to individual events.

### Difference between [`stats`](ppl-stats-command.md) and `eventstats`
The `stats` and `eventstats` commands are both used for calculating statistics, but they have some key differences in how they operate and what they produce:

- Output Format:
  - `stats`: Produces a summary table with only the calculated statistics.
  - `eventstats`: Adds the calculated statistics as new fields to the existing events, preserving the original data.
- Event Retention:
  - `stats`: Reduces the result set to only the statistical summary, discarding individual events.
  - `eventstats`: Retains all original events and adds new fields with the calculated statistics.
- Use Cases:
  - `stats`: Best for creating summary reports or dashboards. Often used as a final command to summarize results.
  - `eventstats`: Useful when you need to enrich events with statistical context for further analysis or filtering. Can be used mid-search to add statistics that can be used in subsequent commands.

### Syntax
`eventstats <aggregation>... [by-clause]`

### **aggregation:**
mandatory. A aggregation function. The argument of aggregation must be field.

**by-clause**: optional.

#### Syntax:
`by [span-expression,] [field,]...`

**Description:**

The by clause could be the fields and expressions like scalar functions and aggregation functions.
Besides, the span clause can be used to split specific field into buckets in the same interval, the eventstats then does the aggregation by these span buckets.

**Default**:

If no `<by-clause>` is specified, the eventstats command aggregates over the entire result set.

### **`span-expression`**:
optional, at most one.

#### Syntax:
`span(field_expr, interval_expr)`

**Description:**

The unit of the interval expression is the natural unit by default.
If the field is a date and time type field, and the interval is in date/time units, you will need to specify the unit in the interval expression.

For example, to split the field ``age`` into buckets by 10 years, it looks like ``span(age, 10)``. And here is another example of time span, the span to split a ``timestamp`` field into hourly intervals, it looks like ``span(timestamp, 1h)``.

* Available time unit:
```
+----------------------------+
| Span Interval Units        |
+============================+
| millisecond (ms)           |
+----------------------------+
| second (s)                 |
+----------------------------+
| minute (m, case sensitive) |
+----------------------------+
| hour (h)                   |
+----------------------------+
| day (d)                    |
+----------------------------+
| week (w)                   |
+----------------------------+
| month (M, case sensitive)  |
+----------------------------+
| quarter (q)                |
+----------------------------+
| year (y)                   |
+----------------------------+
```

### Aggregation Functions

#### _COUNT_

**Description**

Returns a count of the number of expr in the rows retrieved by a SELECT statement.

Example:

    os> source=accounts | eventstats count();
    fetched rows / total rows = 4/4
    +----------------+----------+-----------+----------+-----+--------+-----------------------+----------+--------------------------+--------+-------+---------+
    | account_number | balance  | firstname | lastname | age | gender | address               | employer | email                    | city   | state | count() |
    +----------------+----------+-----------+----------+-----+--------+-----------------------+----------+--------------------------+--------+-------+---------+
    | 1              | 39225    | Amber     | Duke     | 32  | M      | 880 Holmes Lane       | Pyrami   | amberduke@pyrami.com     | Brogan | IL    | 4       |
    | 6              | 5686     | Hattie    | Bond     | 36  | M      | 671 Bristol Street    | Netagy   | hattiebond@netagy.com    | Dante  | TN    | 4       |
    | 13             | 32838    | Nanette   | Bates    | 28  | F      | 789 Madison Street    | Quility  |                          | Nogal  | VA    | 4       |
    | 18             | 4180     | Dale      | Adams    | 33  | M      | 467 Hutchinson Court  |          | daleadams@boink.com      | Orick  | MD    | 4       |
    +----------------+----------+-----------+----------+-----+--------+-----------------------+----------+--------------------------+--------+-------+---------+


#### _SUM_

**Description**

`SUM(expr)`. Returns the sum of expr.

Example:

    os> source=accounts | eventstats sum(age) by gender;
    fetched rows / total rows = 4/4
    +----------------+----------+-----------+----------+-----+--------+-----------------------+----------+--------------------------+--------+-------+--------------------+
    | account_number | balance  | firstname | lastname | age | gender | address               | employer | email                    | city   | state | sum(age) by gender |
    +----------------+----------+-----------+----------+-----+--------+-----------------------+----------+--------------------------+--------+-------+--------------------+
    | 1              | 39225    | Amber     | Duke     | 32  | M      | 880 Holmes Lane       | Pyrami   | amberduke@pyrami.com     | Brogan | IL    | 101                |
    | 6              | 5686     | Hattie    | Bond     | 36  | M      | 671 Bristol Street    | Netagy   | hattiebond@netagy.com    | Dante  | TN    | 101                |
    | 13             | 32838    | Nanette   | Bates    | 28  | F      | 789 Madison Street    | Quility  |                          | Nogal  | VA    | 28                 |
    | 18             | 4180     | Dale      | Adams    | 33  | M      | 467 Hutchinson Court  |          | daleadams@boink.com      | Orick  | MD    | 101                |
    +----------------+----------+-----------+----------+-----+--------+-----------------------+----------+--------------------------+--------+-------+--------------------+


#### _AVG_

**Description**

`AVG(expr)`. Returns the average value of expr.

Example:

    os> source=accounts | eventstats avg(age) by gender;
    fetched rows / total rows = 4/4
    +----------------+----------+-----------+----------+-----+--------+-----------------------+----------+--------------------------+--------+-------+--------------------+
    | account_number | balance  | firstname | lastname | age | gender | address               | employer | email                    | city   | state | avg(age) by gender |
    +----------------+----------+-----------+----------+-----+--------+-----------------------+----------+--------------------------+--------+-------+--------------------+
    | 1              | 39225    | Amber     | Duke     | 32  | M      | 880 Holmes Lane       | Pyrami   | amberduke@pyrami.com     | Brogan | IL    | 33.67              |
    | 6              | 5686     | Hattie    | Bond     | 36  | M      | 671 Bristol Street    | Netagy   | hattiebond@netagy.com    | Dante  | TN    | 33.67              |
    | 13             | 32838    | Nanette   | Bates    | 28  | F      | 789 Madison Street    | Quility  |                          | Nogal  | VA    | 28.00              |
    | 18             | 4180     | Dale      | Adams    | 33  | M      | 467 Hutchinson Court  |          | daleadams@boink.com      | Orick  | MD    | 33.67              |
    +----------------+----------+-----------+----------+-----+--------+-----------------------+----------+--------------------------+--------+-------+--------------------+


#### MAX

**Description**

`MAX(expr)` Returns the maximum value of expr.

Example:

    os> source=accounts | eventstats max(age);
    fetched rows / total rows = 4/4
    +----------------+----------+-----------+----------+-----+--------+-----------------------+----------+--------------------------+--------+-------+-----------+
    | account_number | balance  | firstname | lastname | age | gender | address               | employer | email                    | city   | state | max(age)  |
    +----------------+----------+-----------+----------+-----+--------+-----------------------+----------+--------------------------+--------+-------+-----------+
    | 1              | 39225    | Amber     | Duke     | 32  | M      | 880 Holmes Lane       | Pyrami   | amberduke@pyrami.com     | Brogan | IL    | 36        |
    | 6              | 5686     | Hattie    | Bond     | 36  | M      | 671 Bristol Street    | Netagy   | hattiebond@netagy.com    | Dante  | TN    | 36        |
    | 13             | 32838    | Nanette   | Bates    | 28  | F      | 789 Madison Street    | Quility  |                          | Nogal  | VA    | 36        |
    | 18             | 4180     | Dale      | Adams    | 33  | M      | 467 Hutchinson Court  |          | daleadams@boink.com      | Orick  | MD    | 36        |
+----------------+----------+-----------+----------+-----+--------+-----------------------+----------+--------------------------+--------+-------+-----------+


#### MIN

**Description**

`MIN(expr)` Returns the minimum value of expr.

Example:

    os> source=accounts | eventstats min(age);
    fetched rows / total rows = 4/4
    +----------------+----------+-----------+----------+-----+--------+-----------------------+----------+--------------------------+--------+-------+-----------+
    | account_number | balance  | firstname | lastname | age | gender | address               | employer | email                    | city   | state | min(age)  |
    +----------------+----------+-----------+----------+-----+--------+-----------------------+----------+--------------------------+--------+-------+-----------+
    | 1              | 39225    | Amber     | Duke     | 32  | M      | 880 Holmes Lane       | Pyrami   | amberduke@pyrami.com     | Brogan | IL    | 28        |
    | 6              | 5686     | Hattie    | Bond     | 36  | M      | 671 Bristol Street    | Netagy   | hattiebond@netagy.com    | Dante  | TN    | 28        |
    | 13             | 32838    | Nanette   | Bates    | 28  | F      | 789 Madison Street    | Quility  |                          | Nogal  | VA    | 28        |
    | 18             | 4180     | Dale      | Adams    | 33  | M      | 467 Hutchinson Court  |          | daleadams@boink.com      | Orick  | MD    | 28        |
    +----------------+----------+-----------+----------+-----+--------+-----------------------+----------+--------------------------+--------+-------+-----------+


#### STDDEV_SAMP

**Description**

`STDDEV_SAMP(expr)` Return the sample standard deviation of expr.

Example:

    os> source=accounts | eventstats stddev_samp(age);
    fetched rows / total rows = 4/4
    +----------------+----------+-----------+----------+-----+--------+-----------------------+----------+--------------------------+--------+-------+------------------------+
    | account_number | balance  | firstname | lastname | age | gender | address               | employer | email                    | city   | state | stddev_samp(age)       |
    +----------------+----------+-----------+----------+-----+--------+-----------------------+----------+--------------------------+--------+-------+------------------------+
    | 1              | 39225    | Amber     | Duke     | 32  | M      | 880 Holmes Lane       | Pyrami   | amberduke@pyrami.com     | Brogan | IL    | 3.304037933599835      |
    | 6              | 5686     | Hattie    | Bond     | 36  | M      | 671 Bristol Street    | Netagy   | hattiebond@netagy.com    | Dante  | TN    | 3.304037933599835      |
    | 13             | 32838    | Nanette   | Bates    | 28  | F      | 789 Madison Street    | Quility  |                          | Nogal  | VA    | 3.304037933599835      |
    | 18             | 4180     | Dale      | Adams    | 33  | M      | 467 Hutchinson Court  |          | daleadams@boink.com      | Orick  | MD    | 3.304037933599835      |
    +----------------+----------+-----------+----------+-----+--------+-----------------------+----------+--------------------------+--------+-------+------------------------+


#### STDDEV_POP

**Description**

`STDDEV_POP(expr)` Return the population standard deviation of expr.

Example:

    os> source=accounts | eventstats stddev_pop(age);
    fetched rows / total rows = 4/4
    +----------------+----------+-----------+----------+-----+--------+-----------------------+----------+--------------------------+--------+-------+------------------------+
    | account_number | balance  | firstname | lastname | age | gender | address               | employer | email                    | city   | state | stddev_pop(age)        |
    +----------------+----------+-----------+----------+-----+--------+-----------------------+----------+--------------------------+--------+-------+------------------------+
    | 1              | 39225    | Amber     | Duke     | 32  | M      | 880 Holmes Lane       | Pyrami   | amberduke@pyrami.com     | Brogan | IL    | 2.8613807855648994     |
    | 6              | 5686     | Hattie    | Bond     | 36  | M      | 671 Bristol Street    | Netagy   | hattiebond@netagy.com    | Dante  | TN    | 2.8613807855648994     |
    | 13             | 32838    | Nanette   | Bates    | 28  | F      | 789 Madison Street    | Quility  |                          | Nogal  | VA    | 2.8613807855648994     |
    | 18             | 4180     | Dale      | Adams    | 33  | M      | 467 Hutchinson Court  |          | daleadams@boink.com      | Orick  | MD    | 2.8613807855648994     |
    +----------------+----------+-----------+----------+-----+--------+-----------------------+----------+--------------------------+--------+-------+------------------------+


#### PERCENTILE or PERCENTILE_APPROX

**Description**

`PERCENTILE(expr, percent)` or `PERCENTILE_APPROX(expr, percent)` Return the approximate percentile value of expr at the specified percentage.

* percent: The number must be a constant between 0 and 100.
---

Examples:

    os> source=accounts | eventstats percentile(age, 90) by gender;
    fetched rows / total rows = 4/4
    +----------------+----------+-----------+----------+-----+--------+-----------------------+----------+--------------------------+--------+-------+--------------------------------+
    | account_number | balance  | firstname | lastname | age | gender | address               | employer | email                    | city   | state | percentile(age, 90) by gender  |
    +----------------+----------+-----------+----------+-----+--------+-----------------------+----------+--------------------------+--------+-------+--------------------------------+
    | 1              | 39225    | Amber     | Duke     | 32  | M      | 880 Holmes Lane       | Pyrami   | amberduke@pyrami.com     | Brogan | IL    | 36                             |
    | 6              | 5686     | Hattie    | Bond     | 36  | M      | 671 Bristol Street    | Netagy   | hattiebond@netagy.com    | Dante  | TN    | 36                             |
    | 13             | 32838    | Nanette   | Bates    | 28  | F      | 789 Madison Street    | Quility  |                          | Nogal  | VA    | 28                             |
    | 18             | 4180     | Dale      | Adams    | 33  | M      | 467 Hutchinson Court  |          | daleadams@boink.com      | Orick  | MD    | 36                             |
    +----------------+----------+-----------+----------+-----+--------+-----------------------+----------+--------------------------+--------+-------+--------------------------------+


### Example 1: Calculate the average, sum and count of a field by group

The example show calculate the average age, sum age and count of events of all the accounts group by gender.

PPL query:

    os> source=accounts | eventstats avg(age) as avg_age, sum(age) as sum_age, count() as count by gender;
    fetched rows / total rows = 4/4
    +----------------+----------+-----------+----------+-----+--------+-----------------------+----------+--------------------------+--------+-------+-----------+-----------+-------+
    | account_number | balance  | firstname | lastname | age | gender | address               | employer | email                    | city   | state | avg_age   | sum_age   | count |
    +----------------+----------+-----------+----------+-----+--------+-----------------------+----------+--------------------------+--------+-------+-----------+-----------+-------+
    | 1              | 39225    | Amber     | Duke     | 32  | M      | 880 Holmes Lane       | Pyrami   | amberduke@pyrami.com     | Brogan | IL    | 33.666667 | 101       | 3     |
    | 6              | 5686     | Hattie    | Bond     | 36  | M      | 671 Bristol Street    | Netagy   | hattiebond@netagy.com    | Dante  | TN    | 33.666667 | 101       | 3     |
    | 13             | 32838    | Nanette   | Bates    | 28  | F      | 789 Madison Street    | Quility  |                          | Nogal  | VA    | 28.000000 | 28        | 1     |
    | 18             | 4180     | Dale      | Adams    | 33  | M      | 467 Hutchinson Court  |          | daleadams@boink.com      | Orick  | MD    | 33.666667 | 101       | 3     |
    +----------------+----------+-----------+----------+-----+--------+-----------------------+----------+--------------------------+--------+-------+-----------+-----------+-------+


### Example 2: Calculate the count by a span

The example gets the count of age by the interval of 10 years.

PPL query:

    os> source=accounts | eventstats count(age) by span(age, 10) as age_span
    fetched rows / total rows = 4/4
    +----------------+----------+-----------+----------+-----+--------+-----------------------+----------+--------------------------+--------+-------+----------+
    | account_number | balance  | firstname | lastname | age | gender | address               | employer | email                    | city   | state | age_span |
    +----------------+----------+-----------+----------+-----+--------+-----------------------+----------+--------------------------+--------+-------+----------+
    | 1              | 39225    | Amber     | Duke     | 32  | M      | 880 Holmes Lane       | Pyrami   | amberduke@pyrami.com     | Brogan | IL    | 3        |
    | 6              | 5686     | Hattie    | Bond     | 36  | M      | 671 Bristol Street    | Netagy   | hattiebond@netagy.com    | Dante  | TN    | 3        |
    | 13             | 32838    | Nanette   | Bates    | 28  | F      | 789 Madison Street    | Quility  |                          | Nogal  | VA    | 1        |
    | 18             | 4180     | Dale      | Adams    | 33  | M      | 467 Hutchinson Court  |          | daleadams@boink.com      | Orick  | MD    | 3        |
    +----------------+----------+-----------+----------+-----+--------+-----------------------+----------+--------------------------+--------+-------+----------+


### Example 3: Calculate the count by a gender and span

The example gets the count of age by the interval of 5 years and group by gender.

PPL query:

    os> source=accounts | eventstats count() as cnt by span(age, 5) as age_span, gender
    fetched rows / total rows = 4/4
    +----------------+----------+-----------+----------+-----+--------+-----------------------+----------+--------------------------+--------+-------+-----+
    | account_number | balance  | firstname | lastname | age | gender | address               | employer | email                    | city   | state | cnt |
    +----------------+----------+-----------+----------+-----+--------+-----------------------+----------+--------------------------+--------+-------+-----+
    | 1              | 39225    | Amber     | Duke     | 32  | M      | 880 Holmes Lane       | Pyrami   | amberduke@pyrami.com     | Brogan | IL    | 2   |
    | 6              | 5686     | Hattie    | Bond     | 36  | M      | 671 Bristol Street    | Netagy   | hattiebond@netagy.com    | Dante  | TN    | 1   |
    | 13             | 32838    | Nanette   | Bates    | 28  | F      | 789 Madison Street    | Quility  |                          | Nogal  | VA    | 1   |
    | 18             | 4180     | Dale      | Adams    | 33  | M      | 467 Hutchinson Court  |          | daleadams@boink.com      | Orick  | MD    | 2   |
    +----------------+----------+-----------+----------+-----+--------+-----------------------+----------+--------------------------+--------+-------+-----+


### Usage
- `source = table | eventstats avg(a) `
- `source = table | where a < 50 | eventstats avg(c) `
- `source = table | eventstats max(c) by b`
- `source = table | eventstats count(c) by b | head 5`
- `source = table | eventstats distinct_count(c)`
- `source = table | eventstats stddev_samp(c)`
- `source = table | eventstats stddev_pop(c)`
- `source = table | eventstats percentile(c, 90)`
- `source = table | eventstats percentile_approx(c, 99)`

**Aggregations With Span**
- `source = table  | eventstats count(a) by span(a, 10) as a_span`
- `source = table  | eventstats sum(age) by span(age, 5) as age_span | head 2`
- `source = table  | eventstats avg(age) by span(age, 20) as age_span, country  | sort - age_span |  head 2`

**Aggregations With TimeWindow Span (tumble windowing function)**

- `source = table | eventstats sum(productsAmount) by span(transactionDate, 1d) as age_date | sort age_date`
- `source = table | eventstats sum(productsAmount) by span(transactionDate, 1w) as age_date, productId`

**Aggregations Group by Multiple Levels**

- `source = table | eventstats avg(age) as avg_state_age by country, state | eventstats avg(avg_state_age) as avg_country_age by country`
- `source = table | eventstats avg(age) as avg_city_age by country, state, city | eval new_avg_city_age = avg_city_age - 1 | eventstats avg(new_avg_city_age) as avg_state_age by country, state | where avg_state_age > 18 | eventstats avg(avg_state_age) as avg_adult_country_age by country`

