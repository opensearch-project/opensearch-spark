## PPL Date and Time Functions

### `ADDDATE`

**Description:**


**Usage:** adddate(date, days) adds the second argument as integer number of days to date.
If days is negative abs(days) are subtracted from date.

Argument type: DATE, LONG

**Return type map:**

(DATE, LONG) -> DATE

Antonyms: `SUBDATE`

Example:

    os> source=people | eval `'2020-08-26' + 1` = ADDDATE(DATE('2020-08-26'), 1) | fields `'2020-08-26' + 1`
    fetched rows / total rows = 1/1
    +--------------------+
    | '2020-08-26' + 1   |
    +--------------------+
    | 2020-08-27         |
    +--------------------+

### `CURDATE`

**Description:**

This function requires Spark 3.4.0+, if you use old Spark version, use `CURRENT_DATE` instead.

Returns the current time as a value in 'YYYY-MM-DD'.
`CURDATE()` returns the time at which it executes as `SYSDATE() <#sysdate>`_ does.

Return type: DATE

Specification: CURDATE() -> DATE

Example:

    > source=people | eval `CURDATE()` = CURDATE() | fields `CURDATE()`
    fetched rows / total rows = 1/1
    +-------------+
    | CURDATE()   |
    |-------------|
    | 2022-08-02  |
    +-------------+


### `CURRENT_DATE`


**Description:**


`CURRENT_DATE()` are synonyms for `CURDATE() <#curdate>`_.

Example:

    > source=people | eval `CURRENT_DATE()` = CURRENT_DATE() | fields `CURRENT_DATE()`
    fetched rows / total rows = 1/1
    +------------------+
    | CURRENT_DATE()   |
    |------------------+
    | 2022-08-02       |
    +------------------+

### `CURRENT_TIMESTAMP`

**Description:**


`CURRENT_TIMESTAMP()` are synonyms for `NOW() <#now>`_.

Example:

    > source=people | eval `CURRENT_TIMESTAMP()` = CURRENT_TIMESTAMP() | fields `CURRENT_TIMESTAMP()`
    fetched rows / total rows = 1/1
    +-----------------------+
    | CURRENT_TIMESTAMP()   |
    |-----------------------+
    | 2022-08-02 15:54:19   |
    +-----------------------+


### `DATE`

**Description:**


**Usage:** date(expr) constructs a date type with the input string expr as a date. If the argument is of date/timestamp, it extracts the date value part from the expression.

Argument type: STRING/DATE/TIMESTAMP

Return type: DATE

Example:

    os> source=people | eval `DATE('2020-08-26')` = DATE('2020-08-26') | fields `DATE('2020-08-26')`
    fetched rows / total rows = 1/1
    +----------------------+
    | DATE('2020-08-26')   |
    |----------------------|
    | 2020-08-26           |
    +----------------------+

    os> source=people | eval `DATE(TIMESTAMP('2020-08-26 13:49:00'))` = DATE(TIMESTAMP('2020-08-26 13:49:00')) | fields `DATE(TIMESTAMP('2020-08-26 13:49:00'))`
    fetched rows / total rows = 1/1
    +------------------------------------------+
    | DATE(TIMESTAMP('2020-08-26 13:49:00'))   |
    |------------------------------------------|
    | 2020-08-26                               |
    +------------------------------------------+

    os> source=people | eval `DATE('2020-08-26 13:49')` = DATE('2020-08-26 13:49') | fields `DATE('2020-08-26 13:49')`
    fetched rows / total rows = 1/1
    +----------------------------+
    | DATE('2020-08-26 13:49')   |
    |----------------------------|
    | 2020-08-26                 |
    +----------------------------+


### `DATE_FORMAT`


**Description:**


**Usage:** date_format(date, format) formats the date argument using the specifiers in the format argument.
If an argument of type TIME is provided, the local date is used.

| Symbol | Meaning                       | Presentation   | Examples                                    |
|--------|-------------------------------|----------------|---------------------------------------------|
| G      | era                           | text           | AD; Anno Domini                             |
| y      | year                          | year           | 2020; 20                                    |
| D      | day-of-year                   | number(3)      | 189                                         |
| M/L    | month-of-year                 | month          | 7; 07; Jul; July                            |
| d      | day-of-month                  | number(3)      | 28                                          |
| Q/q    | quarter-of-year               | number/text    | 3; 03; Q3; 3rd quarter                      |
| E      | day-of-week                   | text           | Tue; Tuesday                                |
| F      | aligned day of week in month  | number(1)      | 3                                           |
| a      | am-pm-of-day                  | am-pm          | PM                                          |
| h      | clock-hour-of-am-pm (1-12)    | number(2)      | 12                                          |
| K      | hour-of-am-pm (0-11)          | number(2)      | 0                                           |
| k      | clock-hour-of-day (1-24)      | number(2)      | 0                                           |
| H      | hour-of-day (0-23)            | number(2)      | 0                                           |
| m      | minute-of-hour                | number(2)      | 30                                          |
| s      | second-of-minute              | number(2)      | 55                                          |
| S      | fraction-of-second            | fraction       | 978                                         |
| V      | time-zone ID                  | zone-id        | America/Los_Angeles; Z; -08:30              |
| z      | time-zone name                | zone-name      | Pacific Standard Time; PST                  |
| O      | localized zone-offset         | offset-O       | GMT+8; GMT+08:00; UTC-08:00                 |
| X      | zone-offset 'Z' for zero      | offset-X       | Z; -08; -0830; -08:30; -083015; -08:30:15   |
| x      | zone-offset                   | offset-x       | +0000; -08; -0830; -08:30; -083015; -08:30:15 |
| Z      | zone-offset                   | offset-Z       | +0000; -0800; -08:00                        |
| [      | optional section start        |                |                                             |
| ]      | optional section end          |                |                                             |


Argument type: STRING/DATE/TIME/TIMESTAMP, STRING

Return type: STRING

Example:

    os> source=people | eval `DATE_FORMAT('1998-01-31 13:14:15.012345', 'HH:mm:ss.SSSSSS')` = DATE_FORMAT('1998-01-31 13:14:15.012345', 'HH:mm:ss.SSSSSS'), `DATE_FORMAT(TIMESTAMP('1998-01-31 13:14:15.012345'), 'yyyy-MMM-dd hh:mm:ss a')` = DATE_FORMAT(TIMESTAMP('1998-01-31 13:14:15.012345'), 'yyyy-MMM-dd hh:mm:ss a') | fields `DATE_FORMAT('1998-01-31 13:14:15.012345', 'HH:mm:ss.SSSSSS')`, `DATE_FORMAT(TIMESTAMP('1998-01-31 13:14:15.012345'), 'yyyy-MMM-dd hh:mm:ss a')`
    fetched rows / total rows = 1/1
    +------------------------------------------------------------------+------------------------------------------------------------------------------------+
    | `DATE_FORMAT('1998-01-31 13:14:15.012345', 'HH:mm:ss.SSSSSS')`   | `DATE_FORMAT(TIMESTAMP('1998-01-31 13:14:15.012345'), 'yyyy-MMM-dd hh:mm:ss a')`   |
    |------------------------------------------------------------------+------------------------------------------------------------------------------------|
    | 13:14:15.012345                                                  | 1998-Jan-31st 01:14:15 PM                                                          |
    +------------------------------------------------------+------------------------------------------------------------------------------------------------+


### `DATEDIFF`

**Usage:** Calculates the difference of date parts of given values. If the first argument is time, today's date is used.

Argument type: DATE/TIMESTAMP, DATE/TIMESTAMP

Return type: LONG

Example:

    os> source=people | eval `'2000-01-02' - '2000-01-01'` = DATEDIFF(TIMESTAMP('2000-01-02 00:00:00'), TIMESTAMP('2000-01-01 23:59:59')), `'2001-02-01' - '2004-01-01'` = DATEDIFF(DATE('2001-02-01'), TIMESTAMP('2004-01-01 00:00:00')), `today - today` = DATEDIFF(TIME('23:59:59'), TIME('00:00:00')) | fields `'2000-01-02' - '2000-01-01'`, `'2001-02-01' - '2004-01-01'`, `today - today`
    fetched rows / total rows = 1/1
    +-------------------------------+-------------------------------+-----------------+
    | '2000-01-02' - '2000-01-01'   | '2001-02-01' - '2004-01-01'   | today - today   |
    |-------------------------------+-------------------------------+-----------------|
    | 1                             | -1064                         | 0               |
    +-------------------------------+-------------------------------+-----------------+


### `DAY`

**Description:**


**Usage:** day(date) extracts the day of the month for date, in the range 1 to 31.

Argument type: STRING/DATE/TIMESTAMP

Return type: INTEGER

Synonyms: `DAYOFMONTH`_, `DAY_OF_MONTH`_

Example:

    os> source=people | eval `DAY(DATE('2020-08-26'))` = DAY(DATE('2020-08-26')) | fields `DAY(DATE('2020-08-26'))`
    fetched rows / total rows = 1/1
    +---------------------------+
    | DAY(DATE('2020-08-26'))   |
    |---------------------------|
    | 26                        |
    +---------------------------+


### `DAYOFMONTH`

**Description:**


**Usage:** 

`dayofmonth(date)` extracts the day of the month for date, in the range 1 to 31.

Argument type: STRING/DATE/TIMESTAMP

Return type: INTEGER

Synonyms: `DAY`_, `DAY_OF_MONTH`_

Example:

    os> source=people | eval `DAYOFMONTH(DATE('2020-08-26'))` = DAYOFMONTH(DATE('2020-08-26')) | fields `DAYOFMONTH(DATE('2020-08-26'))`
    fetched rows / total rows = 1/1
    +----------------------------------+
    | DAYOFMONTH(DATE('2020-08-26'))   |
    |----------------------------------|
    | 26                               |
    +----------------------------------+


### `DAY_OF_MONTH`

**Description:**


**Usage:** 

`day_of_month(date)` extracts the day of the month for date, in the range 1 to 31.

Argument type: STRING/DATE/TIMESTAMP

Return type: INTEGER

Synonyms: `DAY`_, `DAYOFMONTH`_

Example:

    os> source=people | eval `DAY_OF_MONTH(DATE('2020-08-26'))` = DAY_OF_MONTH(DATE('2020-08-26')) | fields `DAY_OF_MONTH(DATE('2020-08-26'))`
    fetched rows / total rows = 1/1
    +------------------------------------+
    | DAY_OF_MONTH(DATE('2020-08-26'))   |
    |------------------------------------|
    | 26                                 |
    +------------------------------------+


### `DAYOFWEEK`

**Description:**


**Usage:** 

`dayofweek(date)` returns the weekday index for date (1 = Sunday, 2 = Monday, ..., 7 = Saturday).

Argument type: STRING/DATE/TIMESTAMP

Return type: INTEGER

Synonyms: `DAY_OF_WEEK`_

Example:

    os> source=people | eval `DAYOFWEEK(DATE('2020-08-26'))` = DAYOFWEEK(DATE('2020-08-26')) | fields `DAYOFWEEK(DATE('2020-08-26'))`
    fetched rows / total rows = 1/1
    +---------------------------------+
    | DAYOFWEEK(DATE('2020-08-26'))   |
    |---------------------------------|
    | 4                               |
    +---------------------------------+


### `DAY_OF_WEEK`


**Description:**


**Usage:** day_of_week(date) returns the weekday index for date (1 = Sunday, 2 = Monday, ..., 7 = Saturday).

Argument type: STRING/DATE/TIMESTAMP

Return type: INTEGER

Synonyms: `DAYOFWEEK`_

Example:

    os> source=people | eval `DAY_OF_WEEK(DATE('2020-08-26'))` = DAY_OF_WEEK(DATE('2020-08-26')) | fields `DAY_OF_WEEK(DATE('2020-08-26'))`
    fetched rows / total rows = 1/1
    +-----------------------------------+
    | DAY_OF_WEEK(DATE('2020-08-26'))   |
    |-----------------------------------|
    | 4                                 |
    +-----------------------------------+


### `DAYOFYEAR`

**Description:**


**Usage:**  

`dayofyear(date)` returns the day of the year for date, in the range 1 to 366.

Argument type: STRING/DATE/TIMESTAMP

Return type: INTEGER

Synonyms: `DAY_OF_YEAR`_

Example:

    os> source=people | eval `DAYOFYEAR(DATE('2020-08-26'))` = DAYOFYEAR(DATE('2020-08-26')) | fields `DAYOFYEAR(DATE('2020-08-26'))`
    fetched rows / total rows = 1/1
    +---------------------------------+
    | DAYOFYEAR(DATE('2020-08-26'))   |
    |---------------------------------|
    | 239                             |
    +---------------------------------+


### `DAY_OF_YEAR`

**Description:**


**Usage:**  day_of_year(date) returns the day of the year for date, in the range 1 to 366.

Argument type: STRING/DATE/TIMESTAMP

Return type: INTEGER

Synonyms: `DAYOFYEAR`_

Example:

    os> source=people | eval `DAY_OF_YEAR(DATE('2020-08-26'))` = DAY_OF_YEAR(DATE('2020-08-26')) | fields `DAY_OF_YEAR(DATE('2020-08-26'))`
    fetched rows / total rows = 1/1
    +-----------------------------------+
    | DAY_OF_YEAR(DATE('2020-08-26'))   |
    |-----------------------------------|
    | 239                               |
    +-----------------------------------+


### `DAYNAME`

**Description:**

This function requires Spark 4.0.0+.

**Usage:**

`dayname(date)` returns the name of the weekday for date, including Monday, Tuesday, Wednesday, Thursday, Friday, Saturday and Sunday.

Argument type: STRING/DATE/TIMESTAMP

Return type: STRING

Example:

    os> source=people | eval `DAYNAME(DATE('2020-08-26'))` = DAYNAME(DATE('2020-08-26')) | fields `DAYNAME(DATE('2020-08-26'))`
    fetched rows / total rows = 1/1
    +-------------------------------+
    | DAYNAME(DATE('2020-08-26'))   |
    |-------------------------------|
    | Wednesday                     |
    +-------------------------------+


### `EARLIEST`

**Description:**

**Usage:** earliest(string, timestamp) returns whether the timestamp defined by the given relative string is earlier
than or at the same time as the given timestamp. See [RELATIVE_TIMESTAMP](#relative_timestamp)
for more details on relative timestamp strings.

Argument type: STRING, TIMESTAMP

Return type: BOOLEAN

Example:

    os> source=relative_datetime | eval timestamp = relative_timestamp(relative_string) | where earliest("now",timestamp) | sort timestamp | fields description, relative_string
    fetched rows / total rows = 3/3
    +--------------+-----------------+
    | description  | relative_string |
    +--------------+-----------------+
    | Now          | NOW             |
    | Tomorrow     | +D@D            |
    | In one month | +month          |
    +--------------+-----------------+

### `FROM_UNIXTIME`

**Description:**


**Usage:** 

Returns a representation of the argument given as a timestamp or character string value. Perform reverse conversion for `UNIX_TIMESTAMP`_ function.
If second argument is provided, it is used to format the result in the same way as the format string used for the `DATE_FORMAT`_ function.
If timestamp is outside of range 1970-01-01 00:00:00 - 3001-01-18 23:59:59.999999 (0 to 32536771199.999999 epoch time), function returns NULL.
Argument type: DOUBLE, STRING

**Return type map:**

DOUBLE -> TIMESTAMP

DOUBLE, STRING -> STRING

Examples:

    os> source=people | eval `FROM_UNIXTIME(1220249547)` = FROM_UNIXTIME(1220249547) | fields `FROM_UNIXTIME(1220249547)`
    fetched rows / total rows = 1/1
    +-----------------------------+
    | FROM_UNIXTIME(1220249547)   |
    |-----------------------------|
    | 2008-09-01 06:12:27         |
    +-----------------------------+

    os> source=people | eval `FROM_UNIXTIME(1220249547, 'HH:mm:ss')` = FROM_UNIXTIME(1220249547, 'HH:mm:ss') | fields `FROM_UNIXTIME(1220249547, 'HH:mm:ss')`
    fetched rows / total rows = 1/1
    +-----------------------------------------+
    | FROM_UNIXTIME(1220249547, 'HH:mm:ss')   |
    |-----------------------------------------|
    | 06:12:27                                |
    +-----------------------------------------+


### `HOUR`

**Description:**


**Usage:**

hour(time) extracts the hour value for time. Different from the time of day value, the time value has a large range and can be greater than 23, so the return value of hour(time) can be also greater than 23.

Argument type: STRING/TIME/TIMESTAMP

Return type: INTEGER

Synonyms: `HOUR_OF_DAY`_

Example:

    os> source=people | eval `HOUR(TIME('01:02:03'))` = HOUR(TIME('01:02:03')) | fields `HOUR(TIME('01:02:03'))`
    fetched rows / total rows = 1/1
    +--------------------------+
    | HOUR(TIME('01:02:03'))   |
    |--------------------------|
    | 1                        |
    +--------------------------+


### `HOUR_OF_DAY`

**Description:**


**Usage:** 

hour_of_day(time) extracts the hour value for time. Different from the time of day value, the time value has a large range and can be greater than 23, so the return value of hour_of_day(time) can be also greater than 23.

Argument type: STRING/TIME/TIMESTAMP

Return type: INTEGER

Synonyms: `HOUR`_

Example:

    os> source=people | eval `HOUR_OF_DAY(TIME('01:02:03'))` = HOUR_OF_DAY(TIME('01:02:03')) | fields `HOUR_OF_DAY(TIME('01:02:03'))`
    fetched rows / total rows = 1/1
    +---------------------------------+
    | HOUR_OF_DAY(TIME('01:02:03'))   |
    |---------------------------------|
    | 1                               |
    +---------------------------------+


### `LAST_DAY`

**Usage:** 

Returns the last day of the month as a DATE for a valid argument.

Argument type: DATE/STRING/TIMESTAMP/TIME

Return type: DATE

Example:

    os> source=people | eval `last_day('2023-02-06')` = last_day('2023-02-06') | fields `last_day('2023-02-06')`
    fetched rows / total rows = 1/1
    +--------------------------+
    | last_day('2023-02-06')   |
    |--------------------------|
    | 2023-02-28               |
    +--------------------------+


### `LATEST`

**Description:**

**Usage:** latest(string, timestamp) returns whether the timestamp defined by the given relative string is later
than or at the same time as the given timestamp. See [RELATIVE_TIMESTAMP](#relative_timestamp)
for more details on relative timestamp strings.

Argument type: STRING, TIMESTAMP

Return type: BOOLEAN

Example:

    os> source=relative_datetime | eval timestamp = relative_timestamp(relative_string) | where latest("now",timestamp) | sort timestamp | fields description, relative_string
    fetched rows / total rows = 3/3
    +---------------+-----------------+
    | description   | relative_string |
    +---------------+-----------------+
    | Two weeks ago | -2wk            |
    | Yesterday     | -1d@d           |
    | Now           | NOW             |
    +---------------+-----------------+


### `LOCALTIMESTAMP`

**Description:**

`LOCALTIMESTAMP()` are synonyms for `NOW() <#now>`_.

Example:

    > source=people | eval `LOCALTIMESTAMP()` = LOCALTIMESTAMP() | fields `LOCALTIMESTAMP()`
    fetched rows / total rows = 1/1
    +---------------------+
    | LOCALTIMESTAMP()    |
    |---------------------+
    | 2022-08-02 15:54:19 |
    +---------------------+


### `LOCALTIME`

**Description:**


`LOCALTIME()` are synonyms for `NOW() <#now>`_.

Example:

    > source=people | eval `LOCALTIME()` = LOCALTIME() | fields `LOCALTIME()`
    fetched rows / total rows = 1/1
    +---------------------+
    | LOCALTIME()         |
    |---------------------+
    | 2022-08-02 15:54:19 |
    +---------------------+


### `MAKE_DATE`


**Description:**


Returns a date, given `year`, `month` and `day` values.
Arguments are rounded to an integer.

**Specifications**:

1. MAKE_DATE(INTEGER, INTEGER, INTEGER) -> DATE

Argument type: INTEGER, INTEGER, INTEGER

Return type: DATE

Example:

    os> source=people | eval `MAKE_DATE(1945, 5, 9)` = MAKEDATE(1945, 5, 9) | fields `MAKEDATE(1945, 5, 9)`
    fetched rows / total rows = 1/1
    +------------------------+
    | MAKEDATE(1945, 5, 9)   |
    |------------------------+
    | 1945-05-09             |
    +------------------------+


### `MINUTE`

**Description:**


**Usage:** minute(time) returns the minute for time, in the range 0 to 59.

Argument type: STRING/TIME/TIMESTAMP

Return type: INTEGER

Synonyms: `MINUTE_OF_HOUR`_

Example:

    os> source=people | eval `MINUTE(TIME('01:02:03'))` =  MINUTE(TIME('01:02:03')) | fields `MINUTE(TIME('01:02:03'))`
    fetched rows / total rows = 1/1
    +----------------------------+
    | MINUTE(TIME('01:02:03'))   |
    |----------------------------|
    | 2                          |
    +----------------------------+


### `MINUTE_OF_HOUR`

**Description:**


**Usage:** minute(time) returns the minute for time, in the range 0 to 59.

Argument type: STRING/TIME/TIMESTAMP

Return type: INTEGER

Synonyms: `MINUTE`_

Example:

    os> source=people | eval `MINUTE_OF_HOUR(TIME('01:02:03'))` =  MINUTE_OF_HOUR(TIME('01:02:03')) | fields `MINUTE_OF_HOUR(TIME('01:02:03'))`
    fetched rows / total rows = 1/1
    +------------------------------------+
    | MINUTE_OF_HOUR(TIME('01:02:03'))   |
    |------------------------------------|
    | 2                                  |
    +------------------------------------+


### `MONTH`

**Description:**

**Usage:** month(date) returns the month for date, in the range 1 to 12 for January to December.

Argument type: STRING/DATE/TIMESTAMP

Return type: INTEGER

Synonyms: `MONTH_OF_YEAR`_

Example:

    os> source=people | eval `MONTH(DATE('2020-08-26'))` =  MONTH(DATE('2020-08-26')) | fields `MONTH(DATE('2020-08-26'))`
    fetched rows / total rows = 1/1
    +-----------------------------+
    | MONTH(DATE('2020-08-26'))   |
    |-----------------------------|
    | 8                           |
    +-----------------------------+


### `MONTH_OF_YEAR`

**Description:**


**Usage:** month_of_year(date) returns the month for date, in the range 1 to 12 for January to December.

Argument type: STRING/DATE/TIMESTAMP

Return type: INTEGER

Synonyms: `MONTH`_

Example:

    os> source=people | eval `MONTH_OF_YEAR(DATE('2020-08-26'))` =  MONTH_OF_YEAR(DATE('2020-08-26')) | fields `MONTH_OF_YEAR(DATE('2020-08-26'))`
    fetched rows / total rows = 1/1
    +-------------------------------------+
    | MONTH_OF_YEAR(DATE('2020-08-26'))   |
    |-------------------------------------|
    | 8                                   |
    +-------------------------------------+


### `MONTHNAME`

This function requires Spark 4.0.0+.

**Description:**


**Usage:** monthname(date) returns the full name of the month for date.

Argument type: STRING/DATE/TIMESTAMP

Return type: STRING

Example:

    os> source=people | eval `MONTHNAME(DATE('2020-08-26'))` = MONTHNAME(DATE('2020-08-26')) | fields `MONTHNAME(DATE('2020-08-26'))`
    fetched rows / total rows = 1/1
    +---------------------------------+
    | MONTHNAME(DATE('2020-08-26'))   |
    |---------------------------------|
    | August                          |
    +---------------------------------+


### `NOW`

**Description:**


Returns the current date and time as a value in 'YYYY-MM-DD hh:mm:ss' format. The value is expressed in the cluster time zone.
`NOW()` returns a constant time that indicates the time at which the statement began to execute. This differs from the behavior for `SYSDATE() <#sysdate>`_, which returns the exact time at which it executes.

Return type: TIMESTAMP

Specification: NOW() -> TIMESTAMP

Example:

    > source=people | eval `value_1` = NOW(), `value_2` = NOW() | fields `value_1`, `value_2`
    fetched rows / total rows = 1/1
    +---------------------+---------------------+
    | value_1             | value_2             |
    |---------------------+---------------------|
    | 2022-08-02 15:39:05 | 2022-08-02 15:39:05 |
    +---------------------+---------------------+


### `QUARTER`

**Description:**


**Usage:** quarter(date) returns the quarter of the year for date, in the range 1 to 4.

Argument type: STRING/DATE/TIMESTAMP

Return type: INTEGER

Example:

    os> source=people | eval `QUARTER(DATE('2020-08-26'))` = QUARTER(DATE('2020-08-26')) | fields `QUARTER(DATE('2020-08-26'))`
    fetched rows / total rows = 1/1
    +-------------------------------+
    | QUARTER(DATE('2020-08-26'))   |
    |-------------------------------|
    | 3                             |
    +-------------------------------+

### `RELATIVE_TIMESTAMP`

**Description:**


**Usage:** relative_timestamp(string) returns a relative timestamp corresponding to the given relative string and the
current timestamp at the time of query execution.

The relative timestamp string has syntax `[+|-]<offset_time_integer><offset_time_unit>@<snap_time_unit>`, and is
made up of two optional components.
* An offset from the current timestamp, which is composed of a sign (`+` or `-`), optional `offset_time_integer`, and 
  `offset_time_unit`. If the offset time integer is not specified, it defaults to `1`. For example, `+2hr` is two
  hours after the current timestamp, while `-mon` is one month ago. 
* A snap-to time using the `@` symbol followed by `snap_time_unit`. The snap-to time is applied after the offset (if 
  specified), and rounds the time <i>down</i> to the start of the specified time unit. For example, `@wk` is the start
  of the current week (Sunday is considered to be the first day of the week).

The special relative timestamp string `now`, corresponding to the current timestamp, is also supported.

The current timestamp is determined once at the start of query execution, and is used for all relative timestamp
calculations for that query. The Spark session time zone (`spark.sql.session.timeZone`) is used for determining relative
timestamps. Offsets using time units (seconds, minutes, or hours) represent a fixed time period; adding twenty-four
hours (`+24h`) will yield a timestamp that is exactly twenty-four hours later, but which may not have the same local
time (because of daylight savings, for example). Conversely, offsets using date units (days, weeks, months, quarters, or
years) do not represent a fixed time period; adding one day (`+1d`) will yield a timestamp with the same local time,
but which may not be exactly twenty-four hours later.

The relative timestamp string is case-insensitive.

The following values are supported for `offset_time_unit`:

| Time Unit | Supported Keywords                        |
|-----------|-------------------------------------------|
| Seconds   | `s`, `sec`, `secs`, `second`, `seconds`   |
| Minutes   | `m`, `min`, `mins`, `minute`, `minutes`   |
| Hours     | `h`, `hr`, `hrs`, `hour`, `hours`         |
| Days      | `d`, `day`, `days`                        |
| Weeks     | `w`, `wk`, `wks`, `week`, `weeks`         |
| Quarters  | `q`, `qtr`, `qtrs`, `quarter`, `quarters` |
| Years     | `y`, `yr`, `yrs`, `year`, `years`         |

All the time units above are supported for `snap_time_unit`, as well as the following day-of-the-week time units:

| Time Unit | Supported Keywords |
|-----------|--------------------|
| Sunday    | `w0`, `w7`         |
| Monday    | `w1`               |
| Tuesday   | `w2`               |
| Wednesday | `w3`               |
| Thursday  | `w4`               |
| Friday    | `w5`               |
| Saturday  | `w6`               |

For example, if the current timestamp is Monday, January 03, 2000 at 01:01:01 am:

| Relative String | Description                                                  | Resulting Relative Time                     |
|-----------------|--------------------------------------------------------------|---------------------------------------------|
| `-60m`          | Sixty minutes ago                                            | Monday, January 03, 2000 at 00:01:01 am     |
| `-1H`           | One hour ago                                                 | Monday, January 03, 2000 at 00:01:01 am     |
| `+2wk`          | Two weeks from now                                           | Monday, January 17, 2000 at 00:01:01 am     |
| `-1h@W3`        | One hour ago, rounded to the start of the previous Wednesday | Wednesday, December 29, 1999 at 00:00:00 am |
| `@d`            | Start of the current day                                     | Monday, January 03, 2000 at 00:00:00 am     |
| `now`           | Now                                                          | Monday, January 03, 2000 at 01:01:01 am     | 

Argument type: STRING

Return type: TIMESTAMP

Example:

    os> source=relative_datetime | eval relative = relative_timestamp(relative_string) | sort relative | fields description, relative_string
    fetched rows / total rows = 1/1
    +---------------+-----------------+
    | description   | relative_string |
    +---------------+-----------------+
    | Two weeks ago | -2wk            |
    | Yesterday     | -1d@d           |
    | Now           | NOW             |
    | Tomorrow      | +D@D            |
    | In one month  | +month          |
    +---------------+-----------------+

### `SECOND`

**Description:**


**Usage:** second(time) returns the second for time, in the range 0 to 59.

Argument type: STRING/TIME/TIMESTAMP

Return type: INTEGER

Synonyms: `SECOND_OF_MINUTE`_

Example:

    os> source=people | eval `SECOND(TIME('01:02:03'))` = SECOND(TIME('01:02:03')) | fields `SECOND(TIME('01:02:03'))`
    fetched rows / total rows = 1/1
    +----------------------------+
    | SECOND(TIME('01:02:03'))   |
    |----------------------------|
    | 3                          |
    +----------------------------+


### `SECOND_OF_MINUTE`

**Description:**


**Usage:** second_of_minute(time) returns the second for time, in the range 0 to 59.

Argument type: STRING/TIME/TIMESTAMP

Return type: INTEGER

Synonyms: `SECOND`_

Example:

    os> source=people | eval `SECOND_OF_MINUTE(TIME('01:02:03'))` = SECOND_OF_MINUTE(TIME('01:02:03')) | fields `SECOND_OF_MINUTE(TIME('01:02:03'))`
    fetched rows / total rows = 1/1
    +--------------------------------------+
    | SECOND_OF_MINUTE(TIME('01:02:03'))   |
    |--------------------------------------|
    | 3                                    |
    +--------------------------------------+


### `SUBDATE`

**Description:**


**Usage:** subdate(date, days) subtracts the second argument as integer number of days from date.

Argument type: DATE/TIMESTAMP, LONG

**Return type map:**

(DATE, LONG) -> DATE

Antonyms: `ADDDATE`

Example:

    os> source=people | eval `'2008-01-02' - 31d` = SUBDATE(DATE('2008-01-02'), 31), `'2020-08-26' - 1` = SUBDATE(DATE('2020-08-26'), 1), `ts '2020-08-26 01:01:01' - 1` = SUBDATE(TIMESTAMP('2020-08-26 01:01:01'), 1) | fields `'2008-01-02' - 31d`, `'2020-08-26' - 1`, `ts '2020-08-26 01:01:01' - 1`
    fetched rows / total rows = 1/1
    +----------------------+--------------------+--------------------------------+
    | '2008-01-02' - 31d   | '2020-08-26' - 1   | ts '2020-08-26 01:01:01' - 1   |
    |----------------------+--------------------+--------------------------------|
    | 2007-12-02 00:00:00  | 2020-08-25         | 2020-08-25 01:01:01            |
    +----------------------+--------------------+--------------------------------+


### `SYSDATE`

**Description:**


Returns the current date and time as a value in 'YYYY-MM-DD hh:mm:ss.nnnnnn'.
SYSDATE() returns the time at which it executes. This differs from the behavior for `NOW() <#now>`_, which returns a constant time that indicates the time at which the statement began to execute.
If the argument is given, it specifies a fractional seconds precision from 0 to 6, the return value includes a fractional seconds part of that many digits.

Optional argument type: INTEGER

Return type: TIMESTAMP

Example:

    > source=people | eval `SYSDATE()` = SYSDATE() | fields `SYSDATE()`
    fetched rows / total rows = 1/1
    +----------------------------+
    | SYSDATE()                  |
    |----------------------------+
    | 2022-08-02 15:39:05.123456 |
    +----------------------------+


### `TIMESTAMP`

**Description:**


**Usage:** timestamp(expr) constructs a timestamp type with the input string `expr` as an timestamp. If the argument is not a string, it casts `expr` to timestamp type with default timezone UTC. If argument is a time, it applies today's date before cast.
With two arguments `timestamp(expr1, expr2)` adds the time expression `expr2` to the date or timestamp expression `expr1` and returns the result as a timestamp value.

Argument type: `STRING/DATE/TIME/TIMESTAMP`

**Return type map:**

(STRING/DATE/TIME/TIMESTAMP) -> TIMESTAMP

(STRING/DATE/TIME/TIMESTAMP, STRING/DATE/TIME/TIMESTAMP) -> TIMESTAMP

Example:

    os> source=people | eval `TIMESTAMP('2020-08-26 13:49:00')` = TIMESTAMP('2020-08-26 13:49:00'), `TIMESTAMP('2020-08-26 13:49:00', TIME('12:15:42'))` = TIMESTAMP('2020-08-26 13:49:00', TIME('12:15:42')) | fields `TIMESTAMP('2020-08-26 13:49:00')`, `TIMESTAMP('2020-08-26 13:49:00', TIME('12:15:42'))`
    fetched rows / total rows = 1/1
    +------------------------------------+------------------------------------------------------+
    | TIMESTAMP('2020-08-26 13:49:00')   | TIMESTAMP('2020-08-26 13:49:00', TIME('12:15:42'))   |
    |------------------------------------+------------------------------------------------------|
    | 2020-08-26 13:49:00                | 2020-08-27 02:04:42                                  |
    +------------------------------------+------------------------------------------------------+


### `UNIX_TIMESTAMP`


**Description:**


**Usage**:

Converts given argument to Unix time (seconds since Epoch - very beginning of year 1970). If no argument given, it returns the current Unix time.
The date argument may be a DATE, or TIMESTAMP string, or a number in YYMMDD, YYMMDDhhmmss, YYYYMMDD, or YYYYMMDDhhmmss format. If the argument includes a time part, it may optionally include a fractional seconds part.
If argument is in invalid format or outside of range 1970-01-01 00:00:00 - 3001-01-18 23:59:59.999999 (0 to 32536771199.999999 epoch time), function returns NULL.
You can use `FROM_UNIXTIME`_ to do reverse conversion.

Argument type: <NONE>/DOUBLE/DATE/TIMESTAMP

Return type: DOUBLE

Example:

    os> source=people | eval `UNIX_TIMESTAMP(double)` = UNIX_TIMESTAMP(20771122143845), `UNIX_TIMESTAMP(timestamp)` = UNIX_TIMESTAMP(TIMESTAMP('1996-11-15 17:05:42')) | fields `UNIX_TIMESTAMP(double)`, `UNIX_TIMESTAMP(timestamp)`
    fetched rows / total rows = 1/1
    +--------------------------+-----------------------------+
    | UNIX_TIMESTAMP(double)   | UNIX_TIMESTAMP(timestamp)   |
    |--------------------------+-----------------------------|
    | 3404817525.0             | 848077542.0                 |
    +--------------------------+-----------------------------+


### `WEEK`

**Description:**

**Usage:** week(date) returns the week number for date.


Argument type: DATE/TIMESTAMP/STRING

Return type: INTEGER

Synonyms: `WEEK_OF_YEAR`_

Example:

    os> source=people | eval `WEEK(DATE('2008-02-20'))` = WEEK(DATE('2008-02-20')) | fields `WEEK(DATE('2008-02-20'))`
    fetched rows / total rows = 1/1
    +----------------------------+
    | WEEK(DATE('2008-02-20'))   |
    |----------------------------+
    | 8                          |
    +----------------------------+


### `WEEKDAY`

**Description:**


**Usage:** weekday(date) returns the weekday index for date (0 = Monday, 1 = Tuesday, ..., 6 = Sunday).

It is similar to the `dayofweek`_ function, but returns different indexes for each day.

Argument type: STRING/DATE/TIME/TIMESTAMP

Return type: INTEGER

Example:

    os> source=people | eval `weekday(DATE('2020-08-26'))` = weekday(DATE('2020-08-26')) | eval `weekday(DATE('2020-08-27'))` = weekday(DATE('2020-08-27')) | fields `weekday(DATE('2020-08-26'))`, `weekday(DATE('2020-08-27'))`
    fetched rows / total rows = 1/1
    +-------------------------------+-------------------------------+
    | weekday(DATE('2020-08-26'))   | weekday(DATE('2020-08-27'))   |
    |-------------------------------+-------------------------------|
    | 2                             | 3                             |
    +-------------------------------+-------------------------------+


### `WEEK_OF_YEAR`

**Description:**


**Usage:** week_of_year(date) returns the week number for date.


Argument type: DATE/TIMESTAMP/STRING

Return type: INTEGER

Synonyms: `WEEK`_

Example:

    os> source=people | eval `WEEK_OF_YEAR(DATE('2008-02-20'))` = WEEK(DATE('2008-02-20'))| fields `WEEK_OF_YEAR(DATE('2008-02-20'))`
    fetched rows / total rows = 1/1
    +------------------------------------+
    | WEEK_OF_YEAR(DATE('2008-02-20'))   |
    |------------------------------------+
    | 8                                  |
    +------------------------------------+


### `YEAR`

**Description:**


**Usage:** year(date) returns the year for date, in the range 1000 to 9999, or 0 for the “zero” date.

Argument type: STRING/DATE/TIMESTAMP

Return type: INTEGER

Example:

    os> source=people | eval `YEAR(DATE('2020-08-26'))` = YEAR(DATE('2020-08-26')) | fields `YEAR(DATE('2020-08-26'))`
    fetched rows / total rows = 1/1
    +----------------------------+
    | YEAR(DATE('2020-08-26'))   |
    |----------------------------|
    | 2020                       |
    +----------------------------+


### `DATE_ADD`

**Description:**

Usage: date_add(date, INTERVAL expr unit) adds the interval expr to date.

Argument type: DATE, INTERVAL

Return type: DATE

Antonyms: `DATE_SUB`

Example::

    os> source=people | eval `'2020-08-26' + 1d` = DATE_ADD(DATE('2020-08-26'), INTERVAL 1 DAY) | fields `'2020-08-26' + 1d`
    fetched rows / total rows = 1/1
    +---------------------+
    | '2020-08-26' + 1d   |
    |---------------------+
    | 2020-08-27          |
    +---------------------+


### `DATE_SUB`

**Description:**

Usage: date_sub(date, INTERVAL expr unit) subtracts the interval expr from date.

Argument type: DATE, INTERVAL

Return type: DATE

Antonyms: `DATE_ADD`

Example::

    os> source=people | eval `'2008-01-02' - 31d` = DATE_SUB(DATE('2008-01-02'), INTERVAL 31 DAY) | fields `'2008-01-02' - 31d`
    fetched rows / total rows = 1/1
    +---------------------+
    | '2008-01-02' - 31d  |
    |---------------------+
    | 2007-12-02          |
    +---------------------+


### `TIMESTAMPADD`

**Description:**

Usage: Returns a TIMESTAMP value based on a passed in DATE/TIMESTAMP/STRING argument and an INTERVAL and INTEGER argument which determine the amount of time to be added.
If the third argument is a STRING, it must be formatted as a valid TIMESTAMP.
If the third argument is a DATE, it will be automatically converted to a TIMESTAMP.

Argument type: INTERVAL, INTEGER, DATE/TIMESTAMP/STRING

INTERVAL must be one of the following tokens: [SECOND, MINUTE, HOUR, DAY, WEEK, MONTH, QUARTER, YEAR]

Examples::

    os> source=people | eval `TIMESTAMPADD(DAY, 17, '2000-01-01 00:00:00')` = TIMESTAMPADD(DAY, 17, '2000-01-01 00:00:00') | eval `TIMESTAMPADD(QUARTER, -1, '2000-01-01 00:00:00')` = TIMESTAMPADD(QUARTER, -1, '2000-01-01 00:00:00') | fields `TIMESTAMPADD(DAY, 17, '2000-01-01 00:00:00')`, `TIMESTAMPADD(QUARTER, -1, '2000-01-01 00:00:00')`
    fetched rows / total rows = 1/1
    +----------------------------------------------+--------------------------------------------------+
    | TIMESTAMPADD(DAY, 17, '2000-01-01 00:00:00') | TIMESTAMPADD(QUARTER, -1, '2000-01-01 00:00:00') |
    |----------------------------------------------+--------------------------------------------------|
    | 2000-01-18 00:00:00                          | 1999-10-01 00:00:00                              |
    +----------------------------------------------+--------------------------------------------------+


### `TIMESTAMPDIFF`

**Description:**

Usage: TIMESTAMPDIFF(interval, start, end) returns the difference between the start and end date/times in interval units.
Arguments will be automatically converted to a TIMESTAMP when appropriate.
Any argument that is a STRING must be formatted as a valid TIMESTAMP.

Argument type: INTERVAL, DATE/TIMESTAMP/STRING, DATE/TIMESTAMP/STRING

INTERVAL must be one of the following tokens: [SECOND, MINUTE, HOUR, DAY, WEEK, MONTH, QUARTER, YEAR]

Examples::

    os> source=people | eval `TIMESTAMPDIFF(YEAR, '1997-01-01 00:00:00', '2001-03-06 00:00:00')` = TIMESTAMPDIFF(YEAR, '1997-01-01 00:00:00', '2001-03-06 00:00:00') | eval `TIMESTAMPDIFF(SECOND, timestamp('1997-01-01 00:00:23'), timestamp('1997-01-01 00:00:00'))` = TIMESTAMPDIFF(SECOND, timestamp('1997-01-01 00:00:23'), timestamp('1997-01-01 00:00:00')) | fields `TIMESTAMPDIFF(YEAR, '1997-01-01 00:00:00', '2001-03-06 00:00:00')`, `TIMESTAMPDIFF(SECOND, timestamp('1997-01-01 00:00:23'), timestamp('1997-01-01 00:00:00'))`
    fetched rows / total rows = 1/1
    +-------------------------------------------------------------------+-------------------------------------------------------------------------------------------+
    | TIMESTAMPDIFF(YEAR, '1997-01-01 00:00:00', '2001-03-06 00:00:00') | TIMESTAMPDIFF(SECOND, timestamp('1997-01-01 00:00:23'), timestamp('1997-01-01 00:00:00')) |
    |-------------------------------------------------------------------+-------------------------------------------------------------------------------------------|
    | 4                                                                 | -23                                                                                       |
    +-------------------------------------------------------------------+-------------------------------------------------------------------------------------------+


### `UTC_TIMESTAMP`

**Description:**

Returns the current UTC timestamp as a value in 'YYYY-MM-DD hh:mm:ss'.

Return type: TIMESTAMP

Specification: UTC_TIMESTAMP() -> TIMESTAMP

Example::

    > source=people | eval `UTC_TIMESTAMP()` = UTC_TIMESTAMP() | fields `UTC_TIMESTAMP()`
    fetched rows / total rows = 1/1
    +---------------------+
    | UTC_TIMESTAMP()     |
    |---------------------|
    | 2022-10-03 17:54:28 |
    +---------------------+


### `CURRENT_TIMEZONE`

**Description:**

Returns the current local timezone.

Return type: STRING

Example::

    > source=people | eval `CURRENT_TIMEZONE()` = CURRENT_TIMEZONE() | fields `CURRENT_TIMEZONE()`
    fetched rows / total rows = 1/1
    +------------------------+
    | CURRENT_TIMEZONE()     |
    |------------------------|
    | America/Chicago        |
    +------------------------+

