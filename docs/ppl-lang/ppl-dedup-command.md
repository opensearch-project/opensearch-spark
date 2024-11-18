## PPL dedup command

### Table of contents

- [Description](#description)
- [Syntax](#syntax)
- [Examples](#examples)
    - [Example 1: Dedup by one field](#example-1-dedup-by-one-field)
    - [Example 2: Keep 2 duplicates documents](#example-2-keep-2-duplicates-documents)
    - [Example 3: Keep or Ignore the empty field by default](#example-3-keep-or-ignore-the-empty-field-by-default)
    - [Example 4: Dedup in consecutive document](#example-4-dedup-in-consecutive-document)
- [Limitation](#limitation)

### Description

Using `dedup` command to remove identical document defined by field from the search result.

### Syntax

```sql
dedup [int] <field-list> [keepempty=<bool>] [consecutive=<bool>]
```

* int: optional. The ``dedup`` command retains multiple events for each combination when you specify <int>. The number for <int> must be greater than 0. If you do not specify a number, only the first occurring event is kept. All other duplicates are removed from the results. **Default:** 1
* keepempty: optional. if true, keep the document if the any field in the field-list has NULL value or field is MISSING. **Default:** false.
* consecutive: optional. If set to true, removes only events with duplicate combinations of values that are consecutive. **Default:** false.
* field-list: mandatory. The comma-delimited field list. At least one field is required.


### Example 1: Dedup by one field

The example show dedup the document with gender field.

PPL query:

    os> source=accounts | dedup gender | fields account_number, gender;
    fetched rows / total rows = 2/2
    +------------------+----------+
    | account_number   | gender   |
    |------------------+----------|
    | 1                | M        |
    | 13               | F        |
    +------------------+----------+

### Example 2: Keep 2 duplicates documents

The example show dedup the document with gender field keep 2 duplication.

PPL query:

    os> source=accounts | dedup 2 gender | fields account_number, gender;
    fetched rows / total rows = 3/3
    +------------------+----------+
    | account_number   | gender   |
    |------------------+----------|
    | 1                | M        |
    | 6                | M        |
    | 13               | F        |
    +------------------+----------+

### Example 3: Keep or Ignore the empty field by default

The example show dedup the document by keep null value field.

PPL query:

    os> source=accounts | dedup email keepempty=true | fields account_number, email;
    fetched rows / total rows = 4/4
    +------------------+-----------------------+
    | account_number   | email                 |
    |------------------+-----------------------|
    | 1                | amberduke@pyrami.com  |
    | 6                | hattiebond@netagy.com |
    | 13               | null                  |
    | 18               | daleadams@boink.com   |
    +------------------+-----------------------+


The example show dedup the document by ignore the empty value field.

PPL query:

    os> source=accounts | dedup email | fields account_number, email;
    fetched rows / total rows = 3/3
    +------------------+-----------------------+
    | account_number   | email                 |
    |------------------+-----------------------|
    | 1                | amberduke@pyrami.com  |
    | 6                | hattiebond@netagy.com |
    | 18               | daleadams@boink.com   |
    +------------------+-----------------------+


### Example 4: Dedup in consecutive document

The example show dedup the consecutive document.

PPL query:

    os> source=accounts | dedup gender consecutive=true | fields account_number, gender;
    fetched rows / total rows = 3/3
    +------------------+----------+
    | account_number   | gender   |
    |------------------+----------|
    | 1                | M        |
    | 13               | F        |
    | 18               | M        |
    +------------------+----------+


### Additional Examples

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

### Limitation:

**Spark Support** ( >= 3.4)

To translate `dedup` command with `allowedDuplication > 1`, such as `| dedup 2 a,b` to Spark plan, the solution is translating to a plan with Window function (e.g row_number) and a new column `row_number_col` as Filter.
 
- For `| dedup 2 a, b keepempty=false`

```
DataFrameDropColumns('_row_number_)
+- Filter ('_row_number_ <= 2) // allowed duplication = 2
   +- Window [row_number() windowspecdefinition('a, 'b, 'a ASC NULLS FIRST, 'b ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS _row_number_], ['a, 'b], ['a ASC NULLS FIRST, 'b ASC NULLS FIRST]
       +- Filter (isnotnull('a) AND isnotnull('b)) // keepempty=false
          +- Project
             +- UnresolvedRelation
```
- For `| dedup 2 a, b keepempty=true`
```
Union
:- DataFrameDropColumns('_row_number_)
:  +- Filter ('_row_number_ <= 2)
:     +- Window [row_number() windowspecdefinition('a, 'b, 'a ASC NULLS FIRST, 'b ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS _row_number_], ['a, 'b], ['a ASC NULLS FIRST, 'b ASC NULLS FIRST]
:        +- Filter (isnotnull('a) AND isnotnull('b))
:           +- Project
:              +- UnresolvedRelation
+- Filter (isnull('a) OR isnull('b))
   +- Project
      +- UnresolvedRelation
```

 - this `dedup` command with `allowedDuplication > 1` feature needs spark version >= 3.4 