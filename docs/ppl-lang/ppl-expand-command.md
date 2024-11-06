## PPL `expand` command

### Description
Using `expand` command to flatten a field of type:
- `Array<Any>`
- `Map<Any>`


### Syntax
`expand <field> [As alias]`

* field: to be expanded (exploded). The field must be of supported type.
* alias: Optional to be expanded as the name to be used instead of the original field name

### Usage Guidelines
The expand command produces a row for each element in the specified array or map field, where:
- Array elements become individual rows.
- Map key-value pairs are broken into separate rows, with each key-value represented as a row.

- When an alias is provided, the exploded values are represented under the alias instead of the original field name.
- This can be used in combination with other commands, such as stats, eval, and parse to manipulate or extract data post-expansion.

### Examples:
-  `source = table | expand employee | stats max(salary) as max by state, company`
-  `source = table | expand employee as worker | stats max(salary) as max by state, company`
-  `source = table | expand employee as worker | eval bonus = salary * 3 | fields worker, bonus`
-  `source = table | expand employee | parse description '(?<email>.+@.+)' | fields employee, email`
-  `source = table | eval array=json_array(1, 2, 3) | expand array as uid | fields name, occupation, uid`
-  `source = table | expand multi_valueA as multiA | expand multi_valueB as multiB`

- Expand command can be used in combination with other commands such as `eval`, `stats` and more
- Using multiple expand commands will create a cartesian product of all the internal elements within each composite array or map 

### Effective SQL push-down query
The expand command is translated into an equivalent SQL operation using LATERAL VIEW explode, allowing for efficient exploding of arrays or maps at the SQL query level.

```sql
SELECT customer exploded_productId
FROM table
LATERAL VIEW explode(productId) AS exploded_productId
```
Where the `explode` command offers the following functionality:
- it is a column operation that returns a new column
- it creates a new row for every element in the exploded column
- internal `null`s are ignored as part of the exploded field (no row is created/exploded for null) 
