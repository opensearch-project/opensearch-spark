## PPL top Command

**Description**
Using ``top`` command to find the most common tuple of values of all fields in the field list.


### Syntax
`top [N] <field-list> [by-clause] [sample(? percent)]`

* N: number of results to return. **Default**: 10
* field-list: mandatory. comma-delimited list of field names.
* by-clause: optional. one or more fields to group the results by.
* sample: optional. allows reducing the amount of fields being scanned using table sample strategy favour velocity over precision 


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

## Example 2: Find the most common values organized by gender using sample strategy

The example finds most common age of all the accounts group by gender sample only 50 % of rows.

PPL query:

    os> source=accounts | top 1 age by gender sample(50 percent);
    fetched rows / total rows = 1/2

