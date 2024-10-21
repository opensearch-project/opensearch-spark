## PPL top Command

**Description**
Using ``top`` command to find the most common tuple of values of all fields in the field list.


### Syntax
`top [N] <field-list> [by-clause]`

* N: number of results to return. **Default**: 10
* field-list: mandatory. comma-delimited list of field names.
* by-clause: optional. one or more fields to group the results by.


### Example 1: Find the most common values in a field

The example finds most common gender of all the accounts.

PPL query:

    os> source=accounts | top gender;
    fetched rows / total rows = 2/2
    +----------+
    | gender   |
    |----------|
    | M        |
    | F        |
    +----------+

### Example 2: Find the most common values in a field

The example finds most common gender of all the accounts.

PPL query:

    os> source=accounts | top 1 gender;
    fetched rows / total rows = 1/1
    +----------+
    | gender   |
    |----------|
    | M        |
    +----------+

### Example 2: Find the most common values organized by gender

The example finds most common age of all the accounts group by gender.

PPL query:

    os> source=accounts | top 1 age by gender;
    fetched rows / total rows = 2/2
    +----------+-------+
    | gender   | age   |
    |----------+-------|
    | F        | 28    |
    | M        | 32    |
    +----------+-------+


### Example 3: Find the top country by occupation using only 75% of the actual data (sampling)

PPL query:

    os> source = account  TABLESAMPLE(75 percent) | top 3 country by occupation

The logical plan outcome of the top queries:

```sql
'Project [*]
+- 'GlobalLimit 3
   +- 'LocalLimit 3
      +- 'Sort ['COUNT('country) AS count_country#68 DESC NULLS LAST], true
         +- 'Aggregate ['country, 'occupation AS occupation#67], ['COUNT('country) AS count_country#66, 'country, 'occupation AS occupation#67]
            +- 'Sample 0.0, 0.75, false, 0
               +- 'UnresolvedRelation [account], [], false

```