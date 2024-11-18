## PPL `flatten` command

### Description
Using `flatten` command to flatten a field of type:
- `struct<?,?>`
- `array<struct<?,?>>`


### Syntax
`flatten <field> [As alias]`

* field: to be flattened. The field must be of supported type.
* alias: to be used as alias for the flattened-output fields. Need to put the alias in brace if there is more than one field.

### Test table
#### Schema
| col\_name | data\_type                                      |
|-----------|-------------------------------------------------|
| \_time    | string                                          |
| bridges   | array\<struct\<length:bigint,name:string\>\>    |
| city      | string                                          |
| coor      | struct\<alt:bigint,lat:double,long:double\>     |
| country   | string                                          |
#### Data
| \_time              | bridges                                      | city    | coor                   | country       |
|---------------------|----------------------------------------------|---------|------------------------|---------------|
| 2024-09-13T12:00:00 | [{801, Tower Bridge}, {928, London Bridge}]  | London  | {35, 51.5074, -0.1278} | England       |
| 2024-09-13T12:00:00 | [{232, Pont Neuf}, {160, Pont Alexandre III}]| Paris   | {35, 48.8566, 2.3522}  | France        |
| 2024-09-13T12:00:00 | [{48, Rialto Bridge}, {11, Bridge of Sighs}] | Venice  | {2, 45.4408, 12.3155}  | Italy         |
| 2024-09-13T12:00:00 | [{516, Charles Bridge}, {343, Legion Bridge}]| Prague  | {200, 50.0755, 14.4378}| Czech Republic|
| 2024-09-13T12:00:00 | [{375, Chain Bridge}, {333, Liberty Bridge}] | Budapest| {96, 47.4979, 19.0402} | Hungary       |
| 1990-09-13T12:00:00 | NULL                                         | Warsaw  | NULL                   | Poland        |



### Example 1: flatten struct
This example shows how to flatten a struct field.
PPL query:
    - `source=table | flatten coor`

| \_time              | bridges                                      | city    | country       | alt | lat    | long   |
|---------------------|----------------------------------------------|---------|---------------|-----|--------|--------|
| 2024-09-13T12:00:00 | [{801, Tower Bridge}, {928, London Bridge}]  | London  | England       | 35  | 51.5074| -0.1278|
| 2024-09-13T12:00:00 | [{232, Pont Neuf}, {160, Pont Alexandre III}]| Paris   | France        | 35  | 48.8566| 2.3522 |
| 2024-09-13T12:00:00 | [{48, Rialto Bridge}, {11, Bridge of Sighs}] | Venice  | Italy         | 2   | 45.4408| 12.3155|
| 2024-09-13T12:00:00 | [{516, Charles Bridge}, {343, Legion Bridge}]| Prague  | Czech Republic| 200 | 50.0755| 14.4378|
| 2024-09-13T12:00:00 | [{375, Chain Bridge}, {333, Liberty Bridge}] | Budapest| Hungary       | 96  | 47.4979| 19.0402|
| 1990-09-13T12:00:00 | NULL                                         | Warsaw  | Poland        | NULL| NULL   | NULL   |



### Example 2: flatten array

The example shows how to flatten an array of struct fields.

PPL query:
    - `source=table | flatten bridges`

| \_time              | city    | coor                   | country       | length | name              |
|---------------------|---------|------------------------|---------------|--------|-------------------|
| 2024-09-13T12:00:00 | London  | {35, 51.5074, -0.1278} | England       | 801    | Tower Bridge      |
| 2024-09-13T12:00:00 | London  | {35, 51.5074, -0.1278} | England       | 928    | London Bridge     |
| 2024-09-13T12:00:00 | Paris   | {35, 48.8566, 2.3522}  | France        | 232    | Pont Neuf         |
| 2024-09-13T12:00:00 | Paris   | {35, 48.8566, 2.3522}  | France        | 160    | Pont Alexandre III|
| 2024-09-13T12:00:00 | Venice  | {2, 45.4408, 12.3155}  | Italy         | 48     | Rialto Bridge     |
| 2024-09-13T12:00:00 | Venice  | {2, 45.4408, 12.3155}  | Italy         | 11     | Bridge of Sighs   |
| 2024-09-13T12:00:00 | Prague  | {200, 50.0755, 14.4378}| Czech Republic| 516    | Charles Bridge    |
| 2024-09-13T12:00:00 | Prague  | {200, 50.0755, 14.4378}| Czech Republic| 343    | Legion Bridge     |
| 2024-09-13T12:00:00 | Budapest| {96, 47.4979, 19.0402} | Hungary       | 375    | Chain Bridge      |
| 2024-09-13T12:00:00 | Budapest| {96, 47.4979, 19.0402} | Hungary       | 333    | Liberty Bridge    |
| 1990-09-13T12:00:00 | Warsaw  | NULL                   | Poland        | NULL   | NULL              |


### Example 3: flatten array and struct
This example shows how to flatten multiple fields.
PPL query:
    - `source=table | flatten bridges | flatten coor`

| \_time              | city    | country       | length | name              | alt  | lat    | long   |
|---------------------|---------|---------------|--------|-------------------|------|--------|--------|
| 2024-09-13T12:00:00 | London  | England       | 801    | Tower Bridge      | 35   | 51.5074| -0.1278|
| 2024-09-13T12:00:00 | London  | England       | 928    | London Bridge     | 35   | 51.5074| -0.1278|
| 2024-09-13T12:00:00 | Paris   | France        | 232    | Pont Neuf         | 35   | 48.8566| 2.3522 |
| 2024-09-13T12:00:00 | Paris   | France        | 160    | Pont Alexandre III| 35   | 48.8566| 2.3522 |
| 2024-09-13T12:00:00 | Venice  | Italy         | 48     | Rialto Bridge     | 2    | 45.4408| 12.3155|
| 2024-09-13T12:00:00 | Venice  | Italy         | 11     | Bridge of Sighs   | 2    | 45.4408| 12.3155|
| 2024-09-13T12:00:00 | Prague  | Czech Republic| 516    | Charles Bridge    | 200  | 50.0755| 14.4378|
| 2024-09-13T12:00:00 | Prague  | Czech Republic| 343    | Legion Bridge     | 200  | 50.0755| 14.4378|
| 2024-09-13T12:00:00 | Budapest| Hungary       | 375    | Chain Bridge      | 96   | 47.4979| 19.0402|
| 2024-09-13T12:00:00 | Budapest| Hungary       | 333    | Liberty Bridge    | 96   | 47.4979| 19.0402|
| 1990-09-13T12:00:00 | Warsaw  | Poland        | NULL   | NULL              | NULL | NULL   | NULL   |

### Example 4: flatten with alias
This example shows how to flatten with alias.
PPL query:
    - `source=table | flatten coor as (altitude, latitude, longitude)`

| \_time              | bridges                                      | city    | country       | altitude | latitude | longtitude |
|---------------------|----------------------------------------------|---------|---------------|----------|----------|------------|
| 2024-09-13T12:00:00 | [{801, Tower Bridge}, {928, London Bridge}]  | London  | England       | 35       | 51.5074  | -0.1278    |
| 2024-09-13T12:00:00 | [{232, Pont Neuf}, {160, Pont Alexandre III}]| Paris   | France        | 35       | 48.8566  | 2.3522     |
| 2024-09-13T12:00:00 | [{48, Rialto Bridge}, {11, Bridge of Sighs}] | Venice  | Italy         | 2        | 45.4408  | 12.3155    |
| 2024-09-13T12:00:00 | [{516, Charles Bridge}, {343, Legion Bridge}]| Prague  | Czech Republic| 200      | 50.0755  | 14.4378    |
| 2024-09-13T12:00:00 | [{375, Chain Bridge}, {333, Liberty Bridge}] | Budapest| Hungary       | 96       | 47.4979  | 19.0402    |
| 1990-09-13T12:00:00 | NULL                                         | Warsaw  | Poland        | NULL     | NULL     | NULL       |
