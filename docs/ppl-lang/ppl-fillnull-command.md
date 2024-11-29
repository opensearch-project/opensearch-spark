## PPL `fillnull` command

### Description
Using ``fillnull`` command to fill null with provided value in one or more fields in the search result.


### Syntax
`fillnull [with <null-replacement> in <nullable-field>["," <nullable-field>]] | [using <source-field> = <null-replacement> [","<source-field> = <null-replacement>]]`

* null-replacement: mandatory. The value used to replace `null`s.
* nullable-field: mandatory. Field reference. The `null` values in the field referred to by the property will be replaced with the values from the null-replacement.


### Example 1: fillnull one field

The example show fillnull one field.

PPL query:

    os> source=logs | fields status_code | eval input=status_code | fillnull with 0 in status_code;
| input | status_code |
|-------|-------------|
| 403   | 403         |
| 403   | 403         |
| NULL  | 0           |
| NULL  | 0           |
| 200   | 200         |
| 404   | 404         |
| 500   | 500         |
| NULL  | 0           |
| 500   | 500         |
| 404   | 404         |
| 200   | 200         |
| 500   | 500         |
| NULL  | 0           |
| NULL  | 0           |
| 404   | 404         |


### Example 2: fillnull applied to multiple fields

The example show fillnull applied to multiple fields.

PPL query:

    os> source=logs | fields request_path, timestamp | eval input_request_path=request_path, input_timestamp = timestamp | fillnull with '???' in request_path, timestamp;
| input_request_path | input_timestamp       | request_path | timestamp              |
|--------------------|-----------------------|--------------|------------------------|
| /contact           | NULL                  | /contact     | ???                    |
| /home              | NULL                  | /home        | ???                    |
| /about             | 2023-10-01 10:30:00   | /about       | 2023-10-01 10:30:00    |
| /home              | 2023-10-01 10:15:00   | /home        | 2023-10-01 10:15:00    |
| NULL               | 2023-10-01 10:20:00   | ???          | 2023-10-01 10:20:00    |
| NULL               | 2023-10-01 11:05:00   | ???          | 2023-10-01 11:05:00    |
| /about             | NULL                  | /about       | ???                    |
| /home              | 2023-10-01 10:00:00   | /home        | 2023-10-01 10:00:00    |
| /contact           | NULL                  | /contact     | ???                    |
| NULL               | 2023-10-01 10:05:00   | ???          | 2023-10-01 10:05:00    |
| NULL               | 2023-10-01 10:50:00   | ???          | 2023-10-01 10:50:00    |
| /services          | NULL                  | /services    | ???                    |
| /home              | 2023-10-01 10:45:00   | /home        | 2023-10-01 10:45:00    |
| /services          | 2023-10-01 11:00:00   | /services    | 2023-10-01 11:00:00    |
| NULL               | 2023-10-01 10:35:00   | ???          | 2023-10-01 10:35:00    |

### Example 3: fillnull applied to multiple fields with various `null` replacement values

The example show fillnull with various values used to replace `null`s.
- `/error` in `request_path` field
- `1970-01-01 00:00:00` in `timestamp` field

PPL query:

    os> source=logs | fields request_path, timestamp | eval input_request_path=request_path, input_timestamp = timestamp | fillnull using request_path = '/error', timestamp='1970-01-01 00:00:00';


| input_request_path | input_timestamp       | request_path | timestamp              |
|--------------------|-----------------------|--------------|------------------------|
| /contact           | NULL                  | /contact     | 1970-01-01 00:00:00    |
| /home              | NULL                  | /home        | 1970-01-01 00:00:00    |
| /about             | 2023-10-01 10:30:00   | /about       | 2023-10-01 10:30:00    |
| /home              | 2023-10-01 10:15:00   | /home        | 2023-10-01 10:15:00    |
| NULL               | 2023-10-01 10:20:00   | /error       | 2023-10-01 10:20:00    |
| NULL               | 2023-10-01 11:05:00   | /error       | 2023-10-01 11:05:00    |
| /about             | NULL                  | /about       | 1970-01-01 00:00:00    |
| /home              | 2023-10-01 10:00:00   | /home        | 2023-10-01 10:00:00    |
| /contact           | NULL                  | /contact     | 1970-01-01 00:00:00    |
| NULL               | 2023-10-01 10:05:00   | /error       | 2023-10-01 10:05:00    |
| NULL               | 2023-10-01 10:50:00   | /error       | 2023-10-01 10:50:00    |
| /services          | NULL                  | /services    | 1970-01-01 00:00:00    |
| /home              | 2023-10-01 10:45:00   | /home        | 2023-10-01 10:45:00    |
| /services          | 2023-10-01 11:00:00   | /services    | 2023-10-01 11:00:00    |
| NULL               | 2023-10-01 10:35:00   | /error       | 2023-10-01 10:35:00    |
