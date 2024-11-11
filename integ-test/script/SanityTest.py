"""
Copyright OpenSearch Contributors
SPDX-License-Identifier: Apache-2.0
"""

import signal
import sys
import requests
import json
import csv
import time
import logging
from datetime import datetime
import pandas as pd
import argparse
from requests.auth import HTTPBasicAuth
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

"""
Environment: python3

Example to use this script:

python SanityTest.py --base-url ${URL_ADDRESS} --username *** --password *** --datasource ${DATASOURCE_NAME} --input-csv test_queries.csv --output-file test_report --max-workers 2 --check-interval 10 --timeout 600

The input file test_queries.csv should contain column: `query`

For more details, please use command:

python SanityTest.py --help

"""

class FlintTester:
  def __init__(self, base_url, username, password, datasource, max_workers, check_interval, timeout, output_file, start_row, end_row, log_level):
    self.base_url = base_url
    self.auth = HTTPBasicAuth(username, password)
    self.datasource = datasource
    self.headers = { 'Content-Type': 'application/json' }
    self.max_workers = max_workers
    self.check_interval = check_interval
    self.timeout = timeout
    self.output_file = output_file
    self.start = start_row - 1 if start_row else None
    self.end = end_row - 1 if end_row else None
    self.log_level = log_level
    self.max_attempts = (int)(timeout / check_interval)
    self.logger = self._setup_logger()
    self.executor = ThreadPoolExecutor(max_workers=self.max_workers)
    self.thread_local = threading.local()
    self.test_results = []

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


  def get_session_id(self):
    if not hasattr(self.thread_local, 'session_id'):
      self.thread_local.session_id = "empty_session_id"
    self.logger.debug(f"get session id {self.thread_local.session_id}")
    return self.thread_local.session_id

  def set_session_id(self, session_id):
    """Reuse the session id for the same thread"""
    self.logger.debug(f"set session id {session_id}")
    self.thread_local.session_id = session_id

  # Call submit API to submit the query
  def submit_query(self, query, session_id="Empty"):
    url = f"{self.base_url}/_plugins/_async_query"
    payload = {
      "datasource": self.datasource,
      "lang": "ppl",
      "query": query,
      "sessionId": session_id
    }
    self.logger.debug(f"Submit query with payload: {payload}")
    response_json = None
    try:
      response = requests.post(url, auth=self.auth, json=payload, headers=self.headers)
      response_json = response.json()
      response.raise_for_status()
      return response_json
    except Exception as e:
      return {"error": str(e), "response": response_json}

  # Call get API to check the query status
  def get_query_result(self, query_id):
    url = f"{self.base_url}/_plugins/_async_query/{query_id}"
    response_json = None
    try:
      response = requests.get(url, auth=self.auth)
      response_json = response.json()
      response.raise_for_status()
      return response_json
    except Exception as e:
      return {"status": "FAILED", "error": str(e), "response": response_json}

  # Call delete API to cancel the query
  def cancel_query(self, query_id):
    url = f"{self.base_url}/_plugins/_async_query/{query_id}"
    response_json = None
    try:
      response = requests.delete(url, auth=self.auth)
      response_json = response.json()
      response.raise_for_status()
      self.logger.info(f"Cancelled query [{query_id}] with info {response.json()}")
      return response_json
    except Exception as e:
      self.logger.warning(f"Cancel query [{query_id}] error: {str(e)}, got response {response_json}")

  # Run the test and return the result
  def run_test(self, query, seq_id, expected_status):
    self.logger.info(f"Starting test: {seq_id}, {query}")
    start_time = datetime.now()
    pre_session_id = self.get_session_id()
    submit_result = self.submit_query(query, pre_session_id)
    if "error" in submit_result:
      self.logger.warning(f"Submit error: {submit_result}")
      return {
        "query_name": seq_id,
        "query": query,
        "expected_status": expected_status,
        "status": "SUBMIT_FAILED",
        "check_status": "SUBMIT_FAILED" == expected_status if expected_status else None,
        "error": submit_result["error"],
        "duration": 0,
        "start_time": start_time,
        "end_time": datetime.now()
      }

    query_id = submit_result["queryId"]
    session_id = submit_result["sessionId"]
    self.logger.info(f"Submit return: {submit_result}")
    if (session_id != pre_session_id):
      self.logger.info(f"Update session id from {pre_session_id} to {session_id}")
      self.set_session_id(session_id)

    test_result = self.check_query_status(query_id)
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()

    return {
      "query_name": seq_id,
      "query": query,
      "query_id": query_id,
      "session_id": session_id,
      "expected_status": expected_status,
      "status": test_result["status"],
      "check_status": test_result["status"] == expected_status if expected_status else None,
      "error": test_result.get("error", ""),
      "result": test_result if test_result["status"] == "SUCCESS" else None,
      "duration": duration,
      "start_time": start_time,
      "end_time": end_time
    }

  # Check the status of the query periodically until it is completed or failed or exceeded the timeout
  def check_query_status(self, query_id):
    query_id = query_id

    for attempt in range(self.max_attempts):
      time.sleep(self.check_interval)
      result = self.get_query_result(query_id)

      if result["status"] == "FAILED" or result["status"] == "SUCCESS":
        return result

    # Cancel the query if it exceeds the timeout
    self.cancel_query(query_id)
    return {
      "status": "TIMEOUT",
      "error": "Query execution exceeded " + str(self.timeout) + " seconds with last status: " + result["status"],
    }

  def run_tests_from_csv(self, csv_file):
    with open(csv_file, 'r') as f:
      reader = csv.DictReader(f)
      queries = [(row['query'], i, row.get('expected_status', None)) for i, row in enumerate(reader, start=1) if row['query'].strip()]

    # Filtering queries based on start and end
    queries = queries[self.start:self.end]

    # Parallel execution
    futures = [self.executor.submit(self.run_test, query, seq_id, expected_status) for query, seq_id, expected_status in queries]
    for future in as_completed(futures):
      result = future.result()
      self.test_results.append(result)

  def generate_report(self):
    self.logger.info("Generating report...")
    total_queries = len(self.test_results)
    successful_queries = sum(1 for r in self.test_results if r['status'] == 'SUCCESS')
    failed_queries = sum(1 for r in self.test_results if r['status'] == 'FAILED')
    submit_failed_queries = sum(1 for r in self.test_results if r['status'] == 'SUBMIT_FAILED')
    timeout_queries = sum(1 for r in self.test_results if r['status'] == 'TIMEOUT')

    # Create report
    report = {
      "summary": {
        "total_queries": total_queries,
        "successful_queries": successful_queries,
        "failed_queries": failed_queries,
        "submit_failed_queries": submit_failed_queries,
        "timeout_queries": timeout_queries,
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
  parser.add_argument("--base-url", required=True, help="Base URL of the service")
  parser.add_argument("--username", required=True, help="Username for authentication")
  parser.add_argument("--password", required=True, help="Password for authentication")
  parser.add_argument("--datasource", required=True, help="Datasource name")
  parser.add_argument("--input-csv", required=True, help="Path to the CSV file containing test queries")
  parser.add_argument("--output-file", required=True, help="Path to the output report file")
  parser.add_argument("--max-workers", type=int, default=2, help="optional, Maximum number of worker threads (default: 2)")
  parser.add_argument("--check-interval", type=int, default=5, help="optional, Check interval in seconds (default: 5)")
  parser.add_argument("--timeout", type=int, default=600, help="optional, Timeout in seconds (default: 600)")
  parser.add_argument("--start-row", type=int, default=None, help="optional, The start row of the query to run, start from 1")
  parser.add_argument("--end-row", type=int, default=None, help="optional, The end row of the query to run, not included")
  parser.add_argument("--log-level", default="INFO", help="optional, Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL, default: INFO)")

  args = parser.parse_args()

  tester = FlintTester(
    base_url=args.base_url,
    username=args.username,
    password=args.password,
    datasource=args.datasource,
    max_workers=args.max_workers,
    check_interval=args.check_interval,
    timeout=args.timeout,
    output_file=args.output_file,
    start_row=args.start_row,
    end_row=args.end_row,
    log_level=args.log_level,
  )

  # Register signal handlers to generate report on interrupt
  signal.signal(signal.SIGINT, lambda sig, frame: signal_handler(sig, frame, tester))
  signal.signal(signal.SIGTERM, lambda sig, frame: signal_handler(sig, frame, tester))

  # Running tests
  tester.run_tests_from_csv(args.input_csv)

  # Gnerate report
  tester.generate_report()

if __name__ == "__main__":
  main()
