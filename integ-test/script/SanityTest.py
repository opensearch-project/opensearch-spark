"""
Copyright OpenSearch Contributors
SPDX-License-Identifier: Apache-2.0
"""

import glob
import signal
import sys
import requests
import json
import csv
import time
import logging
from datetime import datetime
import argparse
from requests.auth import HTTPBasicAuth
from pyspark.sql import SparkSession
import threading
import pandas as pd

"""
Environment: python3

Example to use this script:

python SanityTest.py --spark-url ${SPARK_URL} --username *** --password *** --opensearch_url ${OPENSEARCH_URL} --input-csv test_queries.csv --output-file test_report

The input file test_queries.csv should contain column: `query`

For more details, please use command:

python SanityTest.py --help

"""

class FlintTester:
  def __init__(self, spark_url, username, password, opensearch_url, output_file, start_row, end_row, log_level):
    self.spark_url = spark_url
    self.auth = HTTPBasicAuth(username, password)
    self.opensearch_url = opensearch_url
    self.output_file = output_file
    self.start = start_row - 1 if start_row else None
    self.end = end_row - 1 if end_row else None
    self.log_level = log_level
    self.logger = self._setup_logger()
    self.test_results = []

    self.spark_client = SparkSession.builder.remote(spark_url).appName("integ-test").getOrCreate()

  def _setup_logger(self):
    logger = logging.getLogger('FlintTester')
    logger.setLevel(self.log_level)

    fh = logging.FileHandler('flint_test.log')
    fh.setLevel(self.log_level)

    ch = logging.StreamHandler()
    ch.setLevel(self.log_level)

    formatter = logging.Formatter(
      '%(asctime)s - %(threadName)s - %(levelname)s - %(message)s'
    )
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)

    logger.addHandler(fh)
    logger.addHandler(ch)

    return logger

  # Create the indices needed for the tests
  def create_indices(self):
    self.logger.info("Creating indices")

    json_files = glob.glob('data/*.json')
    mapping_files = [f for f in json_files if f.endswith('.mapping.json')]
    data_files = [f for f in json_files if not f.endswith('.mapping.json')]
    existing_indices = set()

    for mapping_file in mapping_files:
      index_name = mapping_file[5 : mapping_file.index('.')]

      self.logger.info(f"Checking if index exists: {index_name}")
      response = requests.get(f'{self.opensearch_url}/{index_name}', auth=self.auth)
      if response.status_code == 200:
        existing_indices.add(index_name)
        continue

      self.logger.info(f"Creating index: {index_name}")

      file_data = open(mapping_file, 'rb').read()
      headers = {'Content-Type': 'application/json'}

      response = requests.put(f'{self.opensearch_url}/{index_name}', auth=self.auth, headers=headers, data=file_data)
      if response.status_code != 200:
        self.logger.error(f'Failed to create index: {index_name}')
        response.raise_for_status()

    for data_file in data_files:
      index_name = data_file[5 : data_file.index('.')]
      if index_name in existing_indices:
        continue

      self.logger.info(f"Populating index: {index_name}")

      file_data = open(data_file, 'rb').read()
      headers = {'Content-Type': 'application/x-ndjson'}

      response = requests.post(f'{self.opensearch_url}/{index_name}/_bulk', auth=self.auth, headers=headers, data=file_data)
      if response.status_code != 200:
        response.raise_for_status()

  # Run the test and return the result
  def run_test(self, query, seq_id, expected_status):
    self.logger.info(f"Starting test: {seq_id}, {query}")
    start_time = datetime.now()

    query_str = query.replace('\n', ' ')
    status = None
    result = None
    error_str = None
    try:
      result = self.spark_client.sql(query_str)
      status = 'SUCCESS'
    except Exception as e:
      status = 'FAILED'
      error_str = str(e)

    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()

    return {
      "query_name": seq_id,
      "query": query,
      "expected_status": expected_status,
      "status": status,
      "check_status": status == expected_status if expected_status else None,
      "error": error_str if error_str else None,
      "result": result,
      "duration": duration,
      "start_time": start_time,
      "end_time": end_time
    }

  def run_tests_from_csv(self, csv_file):
    with open(csv_file, 'r') as f:
      reader = csv.DictReader(f)
      queries = [(row['query'], i, row.get('expected_status', None)) for i, row in enumerate(reader, start=1) if row['query'].strip()]

    # Filtering queries based on start and end
    queries = queries[self.start:self.end]

    self.test_results = []
    for query in queries:
      self.test_results.append(self.run_test(query[0], query[1], query[2]))

  def generate_report(self):
    self.logger.info("Generating report...")
    total_queries = len(self.test_results)
    successful_queries = sum(1 for r in self.test_results if r['status'] == 'SUCCESS')
    failed_queries = sum(1 for r in self.test_results if r['status'] == 'FAILED')

    # Create report
    report = {
      "summary": {
        "total_queries": total_queries,
        "successful_queries": successful_queries,
        "failed_queries": failed_queries,
        "execution_time": sum(r['duration'] for r in self.test_results)
      },
      "detailed_results": self.test_results
    }

    # Save report to JSON file
    with open(f"{self.output_file}.json", 'w') as f:
      json.dump(report, f, indent=2, default=str)

    # Save reults to Excel file
    df = pd.DataFrame(self.test_results)
    df.to_excel(f"{self.output_file}.xlsx", index=False)

    self.logger.info(f"Generated report in {self.output_file}.xlsx and {self.output_file}.json")

def signal_handler(sig, frame, tester):
  print(f"Signal {sig} received, generating report...")
  try:
    tester.executor.shutdown(wait=False, cancel_futures=True)
    tester.generate_report()
  finally:
    sys.exit(0)

def main():
  # Parse command line arguments
  parser = argparse.ArgumentParser(description="Run tests from a CSV file and generate a report.")
  parser.add_argument("--spark-url", required=True, help="URL of the Spark service")
  parser.add_argument("--username", required=True, help="Username for authentication")
  parser.add_argument("--password", required=True, help="Password for authentication")
  parser.add_argument("--opensearch-url", required=True, help="URL of the OpenSearch service")
  parser.add_argument("--input-csv", required=True, help="Path to the CSV file containing test queries")
  parser.add_argument("--output-file", required=True, help="Path to the output report file")
  parser.add_argument("--start-row", type=int, default=None, help="optional, The start row of the query to run, start from 1")
  parser.add_argument("--end-row", type=int, default=None, help="optional, The end row of the query to run, not included")
  parser.add_argument("--log-level", default="INFO", help="optional, Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL, default: INFO)")

  args = parser.parse_args()

  tester = FlintTester(
    spark_url=args.spark_url,
    username=args.username,
    password=args.password,
    opensearch_url=args.opensearch_url,
    output_file=args.output_file,
    start_row=args.start_row,
    end_row=args.end_row,
    log_level=args.log_level,
  )

  # Register signal handlers to generate report on interrupt
  signal.signal(signal.SIGINT, lambda sig, frame: signal_handler(sig, frame, tester))
  signal.signal(signal.SIGTERM, lambda sig, frame: signal_handler(sig, frame, tester))

  # Create indices
  tester.create_indices()

  # Running tests
  tester.run_tests_from_csv(args.input_csv)

  # Gnerate report
  tester.generate_report()

if __name__ == "__main__":
  main()
