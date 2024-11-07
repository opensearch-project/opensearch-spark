## PPL trendline Command

**Description**
Using ``trendline`` command to calculate moving averages of fields.

### Syntax - SMA (Simple Moving Average)
`TRENDLINE [sort <[+|-] sort-field>] SMA(number-of-datapoints, field) [AS alias] [SMA(number-of-datapoints, field) [AS alias]]...`

* [+|-]: optional. The plus [+] stands for ascending order and NULL/MISSING first and a minus [-] stands for descending order and NULL/MISSING last. **Default:** ascending order and NULL/MISSING first.
* sort-field: mandatory when sorting is used. The field used to sort.
* number-of-datapoints: mandatory. number of datapoints to calculate the moving average (must be greater than zero).
* field: mandatory. the name of the field the moving average should be calculated for.
* alias: optional. the name of the resulting column containing the moving average.

It is calculated like

    f[i]: The value of field 'f' in the i-th data-point
    n: The number of data-points in the moving window (period)
    t: The current time index

    SMA(t) = (1/n) * Σ(f[i]), where i = t-n+1 to t

#### Example 1: Calculate simple moving average for a timeseries of temperatures

The example calculates the simple moving average over temperatures using two datapoints.

PPL query:

    os> source=t | trendline sma(2, temperature) as temp_trend;
    fetched rows / total rows = 5/5
    +-----------+---------+--------------------+----------+
    |temperature|device-id|           timestamp|temp_trend|
    +-----------+---------+--------------------+----------+
    |         12|     1492|2023-04-06 17:07:...|      NULL|
    |         12|     1492|2023-04-06 17:07:...|      12.0|
    |         13|      256|2023-04-06 17:07:...|      12.5|
    |         14|      257|2023-04-06 17:07:...|      13.5|
    |         15|      258|2023-04-06 17:07:...|      14.5|
    +-----------+---------+--------------------+----------+

#### Example 2: Calculate simple moving averages for a timeseries of temperatures with sorting

The example calculates two simple moving average over temperatures using two and three datapoints sorted descending by device-id.

PPL query:

    os> source=t | trendline sort - device-id sma(2, temperature) as temp_trend_2 sma(3, temperature) as temp_trend_3;
    fetched rows / total rows = 5/5
    +-----------+---------+--------------------+------------+------------------+
    |temperature|device-id|           timestamp|temp_trend_2|      temp_trend_3|
    +-----------+---------+--------------------+------------+------------------+
    |         15|      258|2023-04-06 17:07:...|        NULL|              NULL|
    |         14|      257|2023-04-06 17:07:...|        14.5|              NULL|
    |         13|      256|2023-04-06 17:07:...|        13.5|              14.0|
    |         12|     1492|2023-04-06 17:07:...|        12.5|              13.0|
    |         12|     1492|2023-04-06 17:07:...|        12.0|12.333333333333334|
    +-----------+---------+--------------------+------------+------------------+


### Syntax - WMA (Weighted Moving Average)
`TRENDLINE sort <[+|-] sort-field> WMA(number-of-datapoints, field) [AS alias] [WMA(number-of-datapoints, field) [AS alias]]...`

* [+|-]: optional. The plus [+] stands for ascending order and NULL/MISSING first and a minus [-] stands for descending order and NULL/MISSING last. **Default:** ascending order and NULL/MISSING first.
* sort-field: mandatory. this field specifies the ordering of data poients when calculating the nth_value aggregation.
* number-of-datapoints: mandatory. number of datapoints to calculate the moving average (must be greater than zero).
* field: mandatory. the name of the field the moving averag should be calculated for.
* alias: optional. the name of the resulting column containing the moving average.

It is calculated like

    f[i]: The value of field 'f' in the i-th data point
    n: The number of data points in the moving window (period)
    t: The current time index
    w[i]: The weight assigned to the i-th data point, typically increasing for more recent points

    WMA(t) = ( Σ from i=t−n+1 to t of (w[i] * f[i]) ) / ( Σ from i=t−n+1 to t of w[i] )

#### Example 1: Calculate weighted moving average for a timeseries of temperatures

The example calculates the simple moving average over temperatures using two datapoints.

PPL query:

    os> source=t | trendline sort timestamp wma(2, temperature) as temp_trend;
    fetched rows / total rows = 5/5
    +-----------+---------+--------------------+----------+
    |temperature|device-id|           timestamp|temp_trend|
    +-----------+---------+--------------------+----------+
    |         12|     1492|2023-04-06 17:07:...|      NULL|
    |         12|     1492|2023-04-06 17:07:...|      12.0|
    |         13|      256|2023-04-06 17:07:...|      12.6|
    |         14|      257|2023-04-06 17:07:...|      13.6|
    |         15|      258|2023-04-06 17:07:...|      14.6|
    +-----------+---------+--------------------+----------+

#### Example 2: Calculate simple moving averages for a timeseries of temperatures with sorting

The example calculates two simple moving average over temperatures using two and three datapoints sorted descending by device-id.

PPL query:

    os> source=t | trendline sort - device-id wma(2, temperature) as temp_trend_2 wma(3, temperature) as temp_trend_3;
    fetched rows / total rows = 5/5
    +-----------+---------+--------------------+------------+------------------+
    |temperature|device-id|           timestamp|temp_trend_2|      temp_trend_3|
    +-----------+---------+--------------------+------------+------------------+
    |         15|      258|2023-04-06 17:07:...|        NULL|              NULL|
    |         14|      257|2023-04-06 17:07:...|        14.3|              NULL|
    |         13|      256|2023-04-06 17:07:...|        13.3|              13.6|
    |         12|     1492|2023-04-06 17:07:...|        12.3|              12.6|
    |         12|     1492|2023-04-06 17:07:...|        12.0|             12.16|
    +-----------+---------+--------------------+------------+------------------+
