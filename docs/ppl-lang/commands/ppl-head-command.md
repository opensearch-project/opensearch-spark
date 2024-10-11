## PPL `head` Command

<table>
  <tr>
    <th style="color:gainsboro;">Spark</th>
    <th style="color:greenyellow;">3.0.0+ </th>
  </tr>
  <tr>
    <th style="color:gainsboro;">Status</th>
    <th style="color:greenyellow;">Stable</th>
  </tr>
  <tr>
    <th style="color:gainsboro;">Introduced In</th>
    <th style="color:lightgreen;">0.4.0</th>
  </tr>
</table>

**Description**
The ``head`` command returns the first N number of specified results after an optional offset in search order.


### Syntax
`head [<size>] [from <offset>]`

* <size>: optional integer. number of results to return. **Default:** 10
* <offset>: integer after optional ``from``. number of results to skip. **Default:** 0

### Example 1: Get first 10 results

The example show maximum 10 results from accounts index.

PPL query:

    os> source=accounts | fields firstname, age | head;
    fetched rows / total rows = 4/4
    +-------------+-------+
    | firstname   | age   |
    |-------------+-------|
    | Amber       | 32    |
    | Hattie      | 36    |
    | Nanette     | 28    |
    | Dale        | 33    |
    +-------------+-------+

### Example 2: Get first N results

The example show first N results from accounts index.

PPL query:

    os> source=accounts | fields firstname, age | head 3;
    fetched rows / total rows = 3/3
    +-------------+-------+
    | firstname   | age   |
    |-------------+-------|
    | Amber       | 32    |
    | Hattie      | 36    |
    | Nanette     | 28    |
    +-------------+-------+

### Example 3: Get first N results after offset M

The example show first N results after offset M from accounts index.

PPL query:

    os> source=accounts | fields firstname, age | head 3 from 1;
    fetched rows / total rows = 3/3
    +-------------+-------+
    | firstname   | age   |
    |-------------+-------|
    | Hattie      | 36    |
    | Nanette     | 28    |
    | Dale        | 33    |
    +-------------+-------+
