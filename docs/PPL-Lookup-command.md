## PPL Lookup Command

## Overview
Lookup command enriches your search data by adding or replacing data from a lookup index (dimension table).
You can extend fields of an index with values from a dimension table, append or replace values when lookup condition is matched.
As an alternative of [Join command](../docs/PPL-Join-command.md), lookup command is more suitable for enriching the source data with a static dataset.


### Syntax of Lookup Command

```sql
SEARCH source=<sourceIndex>
| <other piped command>
| LOOKUP <lookupIndex> (<lookupMappingField> [AS <sourceMappingField>])...
    [(REPLACE | APPEND) (<inputField> [AS <outputField>])...]
| <other piped command>
```
**lookupIndex**
- Required
- Description: the name of lookup index (dimension table)

**lookupMappingField**
- Required
- Description: A mapping key in \<lookupIndex\>, analogy to a join key from right table. You can specify multiple \<lookupMappingField\> with comma-delimited.

**sourceMappingField**
- Optional
- Default: \<lookupMappingField\>
- Description: A mapping key from source **query**, analogy to a join key from left side. If you don't specify any \<sourceMappingField\>, its default value is \<lookupMappingField\>.

**inputField**
- Optional
- Default: All fields of \<lookupIndex\> where matched values are applied to result output if no field is specified.
- Description: A field in \<lookupIndex\> where matched values are applied to result output. You can specify multiple \<inputField\> with comma-delimited. If you don't specify any \<inputField\>, all fields of \<lookupIndex\> where matched values are applied to result output.

**outputField**
- Optional
- Default: \<inputField\>
- Description:  A field of output. You can specify multiple \<outputField\>. If you specify \<outputField\> with an existing field name in source query, its values will be replaced or appended by matched values from \<inputField\>. If the field specified in \<outputField\> is a new field, an extended new field will be applied to the results.

**REPLACE | APPEND**
- Optional
- Default: REPLACE
- Description: If you specify REPLACE, matched values in \<lookupIndex\> field overwrite the values in result. If you specify APPEND, matched values in \<lookupIndex\> field only append to the missing values in result.

### Usage
> LOOKUP <lookupIndex> id AS cid REPLACE mail AS email</br>
> LOOKUP <lookupIndex> name REPLACE mail AS email</br>
> LOOKUP <lookupIndex> id AS cid, name APPEND address, mail AS email</br>
> LOOKUP <lookupIndex> id</br>

### Example
```sql
SEARCH source=<sourceIndex>
| WHERE orderType = 'Cancelled'
| LOOKUP account_list, mkt_id AS mkt_code REPLACE amount, account_name AS name
| STATS count(mkt_code), avg(amount) BY name
```
```sql
SEARCH source=<sourceIndex>
| DEDUP market_id
| EVAL category=replace(category, "-", ".")
| EVAL category=ltrim(category, "dvp.")
| LOOKUP bounce_category category AS category APPEND classification
```
```sql
SEARCH source=<sourceIndex>
| LOOKUP bounce_category category
```
