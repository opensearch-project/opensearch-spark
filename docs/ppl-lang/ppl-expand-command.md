## PPL `expand` command

### Description
Using `expand` command to flatten a field of type:
- `Array<Any>`
- `Map<Any>`


### Syntax
`expand <field> [As alias]`

* field: to be expanded (exploded). The field must be of supported type.
* alias: Optional to be expanded as the name to be used instead of the original field name

### Test table

#### Schema
| col\_name | data\_type                                   |
|-----------|----------------------------------------------|
| \_time    | string                                       |
| bridges   | array\<struct\<length:bigint,name:string\>\> |
| city      | string                                       |
| country   | string                                       |

#### Data
| \_time              | bridges                                      | city    | country        |
|---------------------|----------------------------------------------|---------|----------------|
| 2024-09-13T12:00:00 | [{801, Tower Bridge}, {928, London Bridge}]  | London  | England        |
| 2024-09-13T12:00:00 | [{232, Pont Neuf}, {160, Pont Alexandre III}]| Paris   | France         |
| 2024-09-13T12:00:00 | [{48, Rialto Bridge}, {11, Bridge of Sighs}] | Venice  | Italy          |
| 2024-09-13T12:00:00 | [{516, Charles Bridge}, {343, Legion Bridge}]| Prague  | Czech Republic |
| 2024-09-13T12:00:00 | [{375, Chain Bridge}, {333, Liberty Bridge}] | Budapest| Hungary        |
| 1990-09-13T12:00:00 | NULL                                         | Warsaw  | Poland         |



### Example 1: expand struct
This example shows how to expand an array of struct field.
PPL query:
    - `source=table | expand bridges as britishBridge | fields britishBridge`

| \_time              | bridges                                      | city    | country       | alt | lat    | long   |
|---------------------|----------------------------------------------|---------|---------------|-----|--------|--------|
| 2024-09-13T12:00:00 | [{801, Tower Bridge}, {928, London Bridge}]  | London  | England       | 35  | 51.5074| -0.1278|
| 2024-09-13T12:00:00 | [{232, Pont Neuf}, {160, Pont Alexandre III}]| Paris   | France        | 35  | 48.8566| 2.3522 |
| 2024-09-13T12:00:00 | [{48, Rialto Bridge}, {11, Bridge of Sighs}] | Venice  | Italy         | 2   | 45.4408| 12.3155|
| 2024-09-13T12:00:00 | [{516, Charles Bridge}, {343, Legion Bridge}]| Prague  | Czech Republic| 200 | 50.0755| 14.4378|
| 2024-09-13T12:00:00 | [{375, Chain Bridge}, {333, Liberty Bridge}] | Budapest| Hungary       | 96  | 47.4979| 19.0402|
| 1990-09-13T12:00:00 | NULL                                         | Warsaw  | Poland        | NULL| NULL   | NULL   |



### Example 2: expand array

The example shows how to expand an array of struct fields.

PPL query:
    - `source=table | expand bridges`

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


### Example 3: expand array and struct
This example shows how to expand multiple fields.
PPL query:
    - `source=table | expand bridges | expand coor`

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