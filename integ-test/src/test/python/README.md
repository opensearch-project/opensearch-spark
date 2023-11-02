# Sanity Script for OpenSearch Queries

This script is designed to perform sanity checks on OpenSearch queries by executing a series of predefined queries against an OpenSearch cluster and validating the responses.

## Requirements

- Python 3.x
- `requests` library (Install using `pip install requests`)

## Configuration

Before running the script, ensure that the `OPENSEARCH_URL` environment variable is set to your OpenSearch cluster's URL.

Example:
```bash
export OPENSEARCH_URL="http://localhost:9200"
```

## Running the Script

The script can be executed with Python 3. To run the script, use the following command:

```bash
python sanity_script.py
```

You can also use the provided bash script `run_sanity.sh` to run the Python script with parameters.

```bash
./run_sanity.sh --run-tables --run-queries --use-date 20230101
```

Make sure to give execution permission to the bash script:

```bash
chmod +x run_sanity.sh
```

### Parameters

The script accepts several optional parameters to control its behavior:

- `--run-tables`: When this flag is set, the script will only execute queries related to table operations.
- `--run-queries`: This flag controls the execution of general queries that are not related to table operations.
- `--date`: A specific date can be provided in `YYYYMMDD` format to replace the `{date}` placeholder in queries.

### Examples

1. Run only table queries:
   ```bash
   python sanity_script.py --run-tables
   ```

2. Run only non-table queries:
   ```bash
   python sanity_script.py --run-queries
   ```

3. Run all queries with a specific date:
   ```bash
   ./run_sanity.sh --run-tables --run-queries --use-date 20231102
   ```

## Output

The script will generate a log file with a timestamp in its name (e.g., `sanity_report_2023-11-02_12-00-00.log`) that contains the results of the sanity checks, including any errors encountered during execution.

## Support

For any queries or issues, please create an issue in the repository or contact the maintainer.
