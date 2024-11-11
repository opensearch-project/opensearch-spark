# Sanity Test Script

### Description
This Python script executes test queries from a CSV file using an asynchronous query API and generates comprehensive test reports.

The script produces two report types:
1. An Excel report with detailed test information for each query
2. A JSON report containing both test result overview and query-specific details

Apart from the basic feature, it also has some advanced functionality includes:
1. Concurrent query execution (note: the async query service has session limits, so use thread workers moderately despite it already supports session ID reuse)
2. Configurable query timeout with periodic status checks and automatic cancellation if timeout occurs.
3. Flexible row selection from the input CSV file, by specifying start row and end row of the input CSV file.
4. Expected status validation when expected_status is present in the CSV
5. Ability to generate partial reports if testing is interrupted

### Usage
To use this script, you need to have Python **3.6** or higher installed. It also requires the following Python libraries:
```shell
pip install requests pandas
```

After getting the requisite libraries, you can run the script with the following command line parameters in your shell:
```shell
python SanityTest.py --base-url ${URL_ADDRESS} --username *** --password *** --datasource ${DATASOURCE_NAME} --input-csv test_cases.csv --output-file test_report --max-workers 2 --check-interval 10 --timeout 600
```
You need to replace the placeholders with your actual values of URL_ADDRESS, DATASOURCE_NAME and USERNAME, PASSWORD for authentication to your endpoint.

For more details of the command line parameters, you can see the help manual via command:
```shell
python SanityTest.py --help   

usage: SanityTest.py [-h] --base-url BASE_URL --username USERNAME --password PASSWORD --datasource DATASOURCE --input-csv INPUT_CSV
                                      --output-file OUTPUT_FILE [--max-workers MAX_WORKERS] [--check-interval CHECK_INTERVAL] [--timeout TIMEOUT]
                                      [--start-row START_ROW] [--end-row END_ROW]

Run tests from a CSV file and generate a report.

options:
  -h, --help            show this help message and exit
  --base-url BASE_URL   Base URL of the service
  --username USERNAME   Username for authentication
  --password PASSWORD   Password for authentication
  --datasource DATASOURCE
                        Datasource name
  --input-csv INPUT_CSV
                        Path to the CSV file containing test queries
  --output-file OUTPUT_FILE
                        Path to the output report file
  --max-workers MAX_WORKERS
                        optional, Maximum number of worker threads (default: 2)
  --check-interval CHECK_INTERVAL
                        optional, Check interval in seconds (default: 10)
  --timeout TIMEOUT     optional, Timeout in seconds (default: 600)
  --start-row START_ROW
                        optional, The start row of the query to run, start from 1
  --end-row END_ROW     optional, The end row of the query to run, not included
  --log-level LOG_LEVEL
                        optional, Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL, default: INFO)
```

### Input CSV File
As claimed in the description, the input CSV file should at least have the column of `query` to run the tests. It also supports an optional column of `expected_status`, the script will check the actual status against the expected status and generate a new column of `check_status` for the check result -- TRUE means the status check passed; FALSE means the status check failed.

We also provide a sample input CSV file `test_cases.csv` for reference. It includes all sanity test cases we have currently in the Flint.

**TODO**: the prerequisite data of the test cases and ingesting process

### Report Explanation
The generated report contains two files:

#### Excel Report
The Excel report provides the test result details of each query, including the query name(i.e. sequence number in the input csv file currently), query itself, expected status, actual status, and whether the status satisfy the expected status or not. 

It provides an error message if the query execution failed, otherwise it provides the query execution result with empty error.

It also provides the query_id, session_id and start/end time for each query, which can be used to debug the query execution in the Flint.

An example of Excel report:

| query_name | query                                                                                                                                                      | expected_status | status  | check_status | error                                                                              | result                                                                                                                                                      | Duration (s) | query_id                      | session_id                   | Start Time           | End Time            |
|------------|------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------|---------|--------------|------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------|-------------------------------|------------------------------|----------------------|---------------------|
| 1          | describe myglue_test.default.http_logs                                                                                                                     | SUCCESS         | SUCCESS | TRUE         |                                                                                    | {'status': 'SUCCESS', 'schema': [{...}, ...], 'datarows': [[...], ...], 'total': 31, 'size': 31}                                                            | 37.51        | SHFEVWxDNnZjem15Z2x1ZV90ZXN0  | RkgzZm0xNlA5MG15Z2x1ZV90ZXN0 | 2024-11-07 13:34:10  | 2024-11-07 13:34:47 |
| 2          | source = myglue_test.default.http_logs \| dedup status CONSECUTIVE=true                                                                                    | SUCCESS         | FAILED  | FALSE        | {"Message":"Fail to run query. Cause: Consecutive deduplication is not supported"} |                                                                                                                                                             | 39.53        | dVNlaVVxOFZrZW15Z2x1ZV90ZXN0  | ZGU2MllVYmI4dG15Z2x1ZV90ZXN0 | 2024-11-07 13:34:10  | 2024-11-07 13:34:49 |
| 3          | source = myglue_test.default.http_logs \| eval res = json_keys(json('{"account_number":1,"balance":39225,"age":32,"gender":"M"}')) \| head 1 \| fields res | SUCCESS         | SUCCESS | TRUE         |                                                                                    | {'status': 'SUCCESS', 'schema': [{'name': 'res', 'type': 'array'}], 'datarows': [[['account_number', 'balance', 'age', 'gender']]], 'total': 1, 'size': 1}  | 12.77        | WHQxaXlVSGtGUm15Z2x1ZV90ZXN0  | RkgzZm0xNlA5MG15Z2x1ZV90ZXN0 | 2024-11-07 13:34:47  | 2024-11-07 13:38:45 |
| ...        | ...                                                                                                                                                        | ...             | ...     | ...          |                                                                                    |                                                                                                                                                             | ...          | ...                           | ...                          | ...                  | ...                 |


#### JSON Report
The JSON report provides the same information as the Excel report, but in JSON format.Additionally, it includes a statistical summary of the test results at the beginning of the report.

An example of JSON report:
```json
{
  "summary": {
    "total_queries": 115,
    "successful_queries": 110,
    "failed_queries": 3,
    "submit_failed_queries": 0,
    "timeout_queries": 2,
    "execution_time": 16793.223807
  },
  "detailed_results": [
    {
      "query_name": 1,
      "query": "source = myglue_test.default.http_logs | stats avg(size)",
      "query_id": "eFZmTlpTa3EyTW15Z2x1ZV90ZXN0",
      "session_id": "bFJDMWxzb2NVUm15Z2x1ZV90ZXN0",
      "status": "SUCCESS",
      "error": "",
      "result": {
        "status": "SUCCESS",
        "schema": [
          {
            "name": "avg(size)",
            "type": "double"
          }
        ],
        "datarows": [
          [
            4654.305710913499
          ]
        ],
        "total": 1,
        "size": 1
      },
      "duration": 170.621145,
      "start_time": "2024-11-07 14:56:13.869226",
      "end_time": "2024-11-07 14:59:04.490371"
    },
    {
      "query_name": 2,
      "query": "source = myglue_test.default.http_logs | eval res = json_keys(json(\u2018{\"teacher\":\"Alice\",\"student\":[{\"name\":\"Bob\",\"rank\":1},{\"name\":\"Charlie\",\"rank\":2}]}')) | head 1 | fields res",
      "query_id": "bjF4Y1VnbXdFYm15Z2x1ZV90ZXN0",
      "session_id": "c3pvU1V6OW8xM215Z2x1ZV90ZXN0",
      "status": "FAILED",
      "error": "{\"Message\":\"Syntax error: \\n[PARSE_SYNTAX_ERROR] Syntax error at or near 'source'.(line 1, pos 0)\\n\\n== SQL ==\\nsource = myglue_test.default.http_logs | eval res = json_keys(json(\u2018{\\\"teacher\\\":\\\"Alice\\\",\\\"student\\\":[{\\\"name\\\":\\\"Bob\\\",\\\"rank\\\":1},{\\\"name\\\":\\\"Charlie\\\",\\\"rank\\\":2}]}')) | head 1 | fields res\\n^^^\\n\"}",
      "result": null,
      "duration": 14.051738,
      "start_time": "2024-11-07 14:59:18.699335",
      "end_time": "2024-11-07 14:59:32.751073"
    },
    {
      "query_name": 2,
      "query": "source = myglue_test.default.http_logs |  eval col1 = size, col2 = clientip | stats avg(col1) by col2",
      "query_id": "azVyMFFORnBFRW15Z2x1ZV90ZXN0",
      "session_id": "VWF0SEtrNWM3bm15Z2x1ZV90ZXN0",
      "status": "TIMEOUT",
      "error": "Query execution exceeded 600 seconds with last status: running",
      "result": null,
      "duration": 673.710946,
      "start_time": "2024-11-07 14:45:00.157875",
      "end_time": "2024-11-07 14:56:13.868821"
    },
    ...
  ]
}
```
