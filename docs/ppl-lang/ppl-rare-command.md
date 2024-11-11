## PPL rare Command

**Description**
Using ``rare`` command to find the least common tuple of values of all fields in the field list.

**Note**: A maximum of 10 results is returned for each distinct tuple of values of the group-by fields.

**Syntax**
`rare [N] <field-list> [by-clause]`
`rare_approx [N] <field-list> [by-clause]`

* N: number of results to return. **Default**: 10
* field-list: mandatory. comma-delimited list of field names.
* by-clause: optional. one or more fields to group the results by.
* rare_approx: approximate count of the rare (n) fields by using estimated [cardinality by HyperLogLog++ algorithm](https://spark.apache.org/docs/3.5.2/sql-ref-functions-builtin.html).


### Example 1: Find the least common values in a field

The example finds least common gender of all the accounts.

PPL query:

    os> source=accounts | rare gender;
    os> source=accounts | rare_approx 10 gender;
    os> source=accounts | rare_approx gender;
    fetched rows / total rows = 2/2
    +----------+
    | gender   |
    |----------|
    | F        |
    | M        |
    +----------+


### Example 2: Find the least common values organized by gender

The example finds least common age of all the accounts group by gender.

PPL query:

    os> source=accounts | rare 5 age by gender;
    os> source=accounts | rare_approx 5 age by gender;
    fetched rows / total rows = 4/4
    +----------+-------+
    | gender   | age   |
    |----------+-------|
    | F        | 28    |
    | M        | 32    |
    | M        | 33    |
    | M        | 36    |
    +----------+-------+
