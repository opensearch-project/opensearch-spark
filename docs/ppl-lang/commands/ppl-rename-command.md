## PPL `rename` command


<table>
  <tr>
    <th style="color:gainsboro;">Spark</th>
    <th style="color:greenyellow;">3.5.1+ </th>
  </tr>
  <tr>
    <th style="color:gainsboro;">Status</th>
    <th style="color:yellow;">Experimental</th>
  </tr>
  <tr>
    <th style="color:gainsboro;">Introduced In</th>
    <th style="color:lightgreen;">0.5.0</th>
  </tr>
</table>


### Description
Using ``rename`` command to rename one or more fields in the search result.


### Syntax
`rename <source-field> AS <target-field>["," <source-field> AS <target-field>]...`

* source-field: mandatory. The name of the field you want to rename.
* field list: mandatory. The name you want to rename to.


### Example 1: Rename one field

The example show rename one field.

PPL query:

    os> source=accounts | rename account_number as an | fields an;
    fetched rows / total rows = 4/4
    +------+
    | an   |
    |------|
    | 1    |
    | 6    |
    | 13   |
    | 18   |
    +------+


### Example 2: Rename multiple fields

The example show rename multiple fields.

PPL query:

    os> source=accounts | rename account_number as an, employer as emp | fields an, emp;
    fetched rows / total rows = 4/4
    +------+---------+
    | an   | emp     |
    |------+---------|
    | 1    | Pyrami  |
    | 6    | Netagy  |
    | 13   | Quility |
    | 18   | null    |
    +------+---------+

### Limitation:

**Spark Support** ( 3.5.1 +)


- Overriding existing field is unsupported:

`source=accounts | grok address '%{NUMBER} %{GREEDYDATA:address}' | fields address`
