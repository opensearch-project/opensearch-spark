## PPL `lookup` command

### Description
Lookup command enriches your search data by adding or replacing data from a lookup index (dimension table).
You can extend fields of an index with values from a dimension table, append or replace values when lookup condition is matched.
As an alternative of [Join command](ppl-join-command), lookup command is more suitable for enriching the source data with a static dataset.


### Syntax

```
LOOKUP <lookupIndex> (<lookupMappingField> [AS <sourceMappingField>])...
       [(REPLACE | APPEND) (<inputField> [AS <outputField>])...]
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
- `LOOKUP <lookupIndex> id AS cid REPLACE mail AS email`
- `LOOKUP <lookupIndex> name REPLACE mail AS email`
- `LOOKUP <lookupIndex> id AS cid, name APPEND address, mail AS email`
- `LOOKUP <lookupIndex> id`

### Examples 1: replace

PPL query:

    os>source=people | LOOKUP work_info uid AS id REPLACE department | head 10
    fetched rows / total rows = 10/10
    +------+-----------+-------------+-----------+--------+------------------+
    | id   | name      | occupation  | country   | salary | department       |
    +------+-----------+-------------+-----------+--------+------------------+
    | 1000 | Daniel    | Teacher     | Canada    | 56486  | CUSTOMER_SERVICE |
    | 1001 | Joseph    | Lawyer      | Denmark   | 135943 | FINANCE          |
    | 1002 | David     | Artist      | Finland   | 60391  | DATA             |
    | 1003 | Charlotte | Lawyer      | Denmark   | 42173  | LEGAL            |
    | 1004 | Isabella  | Veterinarian| Australia | 117699 | MARKETING        |
    | 1005 | Lily      | Engineer    | Italy     | 37526  | IT               |
    | 1006 | Emily     | Dentist     | Denmark   | 125340 | MARKETING        |
    | 1007 | James     | Lawyer      | Germany   | 56532  | LEGAL            |
    | 1008 | Lucas     | Lawyer      | Japan     | 87782  | DATA             |
    | 1009 | Sophia    | Architect   | Sweden    | 37597  | MARKETING        |
    +------+-----------+-------------+-----------+--------+------------------+

### Examples 2: append

PPL query:

    os>source=people| LOOKUP work_info uid AS ID, name APPEND department | where isnotnull(department) | head 10
    fetched rows / total rows = 10/10
    +------+---------+-------------+-------------+--------+------------+
    | id   | name    | occupation  | country     | salary | department |
    +------+---------+-------------+-------------+--------+------------+
    | 1018 | Emma    | Architect   | USA         | 72400  | IT         |
    | 1032 | James   | Pilot       | Netherlands | 71698  | SALES      |
    | 1043 | Jane    | Nurse       | Brazil      | 45016  | FINANCE    |
    | 1046 | Joseph  | Pharmacist  | Mexico      | 109152 | OPERATIONS |
    | 1064 | Joseph  | Electrician | New Zealand | 50253  | LEGAL      |
    | 1090 | Matthew | Psychologist| Germany     | 73396  | DATA       |
    | 1103 | Emily   | Electrician | Switzerland | 98391  | DATA       |
    | 1114 | Jake    | Nurse       | Denmark     | 53418  | SALES      |
    | 1115 | Sofia   | Engineer    | Mexico      | 64829  | OPERATIONS |
    | 1122 | Oliver  | Scientist   | Netherlands | 31146  | DATA       |
    +------+---------+-------------+-------------+--------+------------+
