# PPL `fields` command

Description
============
Using ``field`` command to keep or remove fields from the search result.


Syntax
============
field [+|-] <field-list>

* index: optional. if the plus (+) is used, only the fields specified in the field list will be keep. if the minus (-) is used, all the fields specified in the field list will be removed. **Default** +
* field list: mandatory. comma-delimited keep or remove fields.


### Example 1: Select specified fields from result

The example show fetch account_number, firstname and lastname fields from search results.

PPL query:

    os> source=accounts | fields account_number, firstname, lastname;
    fetched rows / total rows = 4/4
    +------------------+-------------+------------+
    | account_number   | firstname   | lastname   |
    |------------------+-------------+------------|
    | 1                | Amber       | Duke       |
    | 6                | Hattie      | Bond       |
    | 13               | Nanette     | Bates      |
    | 18               | Dale        | Adams      |
    +------------------+-------------+------------+

### Example 2: Remove specified fields from result

The example show fetch remove account_number field from search results.

PPL query:

    os> source=accounts | fields account_number, firstname, lastname | fields - account_number ;
    fetched rows / total rows = 4/4
    +-------------+------------+
    | firstname   | lastname   |
    |-------------+------------|
    | Amber       | Duke       |
    | Hattie      | Bond       |
    | Nanette     | Bates      |
    | Dale        | Adams      |
    +-------------+------------+

### Additional Examples

- `source = table`
- `source = table | fields a,b,c`
- `source = table | fields + a,b,c`
- `source = table | fields - b,c`
- `source = table | eval b1 = b | fields - b1,c`

### Limitation: 
 - `fields - list` shows incorrect results for spark version 3.3 - see [issue](https://github.com/opensearch-project/opensearch-spark/pull/732)
 - new field added by eval command with a function cannot be dropped in current version:**_

```sql
 `source = table | eval b1 = b + 1 | fields - b1,c` (Field `b1` cannot be dropped caused by SPARK-49782)
 `source = table | eval b1 = lower(b) | fields - b1,c` (Field `b1` cannot be dropped caused by SPARK-49782)
```

**Nested-Fields**
 - nested field shows incorrect results for spark version 3.3 - see [issue](https://github.com/opensearch-project/opensearch-spark/issues/739) 
```sql
`source = catalog.schema.table1, catalog.schema.table2 | fields A.nested1, B.nested1`
`source = catalog.table | where struct_col2.field1.subfield > 'valueA' | sort int_col | fields  int_col, struct_col.field1.subfield, struct_col2.field1.subfield`
`source = catalog.schema.table | where struct_col2.field1.subfield > 'valueA' | sort int_col | fields  int_col, struct_col.field1.subfield, struct_col2.field1.subfield`
```
---