# PPL `eval` command

## Description
 The ``eval`` command evaluate the expression and append the result to the search result.


## Syntax
```sql
eval <field>=<expression> ["," <field>=<expression> ]...
```
* field: mandatory. If the field name not exist, a new field is added. If the field name already exists, it will be overrided.
* expression: mandatory. Any expression support by the system.

### Example 1: Create the new field

The example show to create new field doubleAge for each document. The new doubleAge is the evaluation result of age multiply by 2.

PPL query:

    os> source=accounts | eval doubleAge = age * 2 | fields age, doubleAge ;
    fetched rows / total rows = 4/4
    +-------+-------------+
    | age   | doubleAge   |
    |-------+-------------|
    | 32    | 64          |
    | 36    | 72          |
    | 28    | 56          |
    | 33    | 66          |
    +-------+-------------+


### Example 2: Override the existing field

The example show to override the exist age field with age plus 1.

PPL query:
```sql
    os> source=accounts | eval age = age + 1 | fields age ;
    fetched rows / total rows = 4/4
    +-------+
    | age   |
    |-------|
    | 33    |
    | 37    |
    | 29    |
    | 34    |
    +-------+
```

### Example 3: Create the new field with field defined in eval

The example show to create a new field ddAge with field defined in eval command. The new field ddAge is the evaluation result of doubleAge multiply by 2, the doubleAge is defined in the eval command.

PPL query:

    os> source=accounts | eval doubleAge = age * 2, ddAge = doubleAge * 2 | fields age, doubleAge, ddAge ;
    fetched rows / total rows = 4/4
    +-------+-------------+---------+
    | age   | doubleAge   | ddAge   |
    |-------+-------------+---------|
    | 32    | 64          | 128     |
    | 36    | 72          | 144     |
    | 28    | 56          | 112     |
    | 33    | 66          | 132     |
    +-------+-------------+---------+

### Additional Examples:
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
- `source = table | eval f = a in ('foo', 'bar') | fields f`
- `source = table | eval f = a not in ('foo', 'bar') | fields f`

Eval with `case` example:
```sql
source = table | eval e = eval status_category =
case(a >= 200 AND a < 300, 'Success',
a >= 300 AND a < 400, 'Redirection',
a >= 400 AND a < 500, 'Client Error',
a >= 500, 'Server Error'
else 'Unknown')
```

Eval with another `case` example:

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

### Limitation:
 - `eval` with comma separated expression needs spark version >= 3.4

 - Overriding existing field is unsupported, following queries throw exceptions with "Reference 'a' is ambiguous"

```sql
- `source = table | eval a = 10 | fields a,b,c`
- `source = table | eval a = a * 2 | stats avg(a)`
- `source = table | eval a = abs(a) | where a > 0`
- `source = table | eval a = signum(a) | where a < 0`
```