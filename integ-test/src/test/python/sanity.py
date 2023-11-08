import requests
import time
import json
import os
import logging
from datetime import datetime
import argparse
import re


url = os.environ.get('OPENSEARCH_URL', "http://opensearch:9200")  # Modify this line
table_name = 'http_logs'
# Generate a timestamp for the file name
timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
filename = f'sanity_report_{timestamp}.log'

# Configure logging
logging.basicConfig(filename=filename,
                    level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
test_result = []
sanity_report = []

def get_current_date_str():
    return datetime.now().strftime('%Y%m%d')

# default date for the test
current_date_str =  get_current_date_str()

def log_to_report(query, status, runtime, reason=None):
    report_entry = {
        "Query": query,
        "Status": status,
        "Runtime (seconds)": runtime,
        "Reason": reason
    }
    sanity_report.append(report_entry)

def print_sanity_report():
    logging.info("\n=========== Sanity Report ===========")
    for entry in sanity_report:
        for key, value in entry.items():
            logging.info(f"{key}: {value}")
        logging.info("------------------------------------")

def enable_repl():
    async_url = url + "/_plugins/_query/settings"
    headers = {'Content-Type': 'application/json'}
    data = {"transient":{"plugins.query.executionengine.spark.session.enabled":"true"}}
    try:
        response = requests.put(async_url, headers=headers, json=data)
        if response.status_code // 100 == 2:
            logging.info(f"http request was successful (2xx status code).")
            return response
        else:
            logging.error(f"http request failed with status code: {response.status_code}")
            raise
    except requests.exceptions.ConnectTimeout as e:
        logging.error(f"ConnectTimeout: {e}")
        raise
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise
def fetch_result(queryId):
    fetch_result_url = f"{url}/_plugins/_async_query/{queryId}"

    response = requests.get(fetch_result_url)
    if response.status_code // 100 == 2:
        logging.info(f"http request was successful (2xx status code).")
        return response
    else:
        logging.info(f"http request failed with status code: {response.status_code}")
        raise Exception("FAILED")

def asnyc_query(query):
    async_url = url + "/_plugins/_async_query"
    headers = {'Content-Type': 'application/json'}
    data = {
        "datasource": "mys3",
        "lang": "sql",
        "query": f"{query}"
    }
    response = requests.post(async_url, headers=headers, json=data)
    if response.status_code // 100 == 2:
        logging.info(f"http request was successful (2xx status code).")
        return response
    else:
        logging.info(f"http request failed with status code: {response.status_code}")
        raise Exception("FAILED")

def create_session():
    query = "select 1"
    logging.info(f"\n======================")
    logging.info(f"[{query}] START")
    logging.info(f"======================")
    start_time = time.time()

    response=asnyc_query(query).json()
    sessionId = response['sessionId']
    queryId = response['queryId']
    logging.info(f"sessionId: {sessionId}")
    while True:
        response = fetch_result(queryId).json()
        logging.info(f"status: {response['status']}")
        if response['status'] == 'SUCCESS':
            query_end_time = time.time()
            logging.info(f"\n======================")
            logging.info(f"[{query}] SUCCESS")
            logging.info(f"   Runtime {query_end_time - start_time} seconds")
            logging.info(f"======================")
            return sessionId
        elif response['status'] == 'FAILED':
            raise Exception("FAILED")
        time.sleep(10)

def asnyc_query_session(query, sessionId):
    async_url = url + "/_plugins/_async_query"
    headers = {'Content-Type': 'application/json'}
    data = {
        "datasource": "mys3",
        "lang": "sql",
        "query": f"{query}",
        "sessionId": f"{sessionId}"
    }
    response = requests.post(async_url, headers=headers, json=data)
    if response.status_code // 100 == 2:
        logging.info(f"http request was successful (2xx status code).")
        return response
    else:
        logging.info(f"http request failed with status code: {response.status_code}")
        raise Exception("FAILED")

def test_repl(expected, query, sessionId):
    logging.info(f"\n========REPL==========")
    logging.info(f"[{query}] START")
    logging.info(f"======================")
    start_time = time.time()

    queryId = asnyc_query_session(query, sessionId).json()['queryId']
    logging.info(f"queryId: {queryId}")
    while True:
        try:
            response = fetch_result(queryId).json()
            logging.debug(f"actual response {response} ")
            logging.info(f"status: {response['status']}")
            if response['status'] == 'SUCCESS':
                query_end_time = time.time()
                runtime = query_end_time - start_time
                if expected(response):
                    log_to_report(query, "SUCCESS", runtime)
                    logging.info(f"\n======================")
                    logging.info(f"[{query}] SUCCESS")
                    logging.info(f"   Runtime {runtime} seconds")
                    logging.info(f"======================")
                else:
                    log_to_report(query, "FAILED", runtime, "Unexpected response")
                    logging.info(json.dumps(response, indent=4))
                break
            elif response['status'] == 'FAILED':
                query_end_time = time.time()
                runtime = query_end_time - start_time
                log_to_report(query, "FAILED", runtime, response.get('reason', 'Unknown'))
                logging.info(f"{response['status']}")
                break
        except Exception as e:
            query_end_time = time.time()
            runtime = query_end_time - start_time
            log_to_report(query, "FAILED", runtime, str(e))
            logging.info(f"{e}")
            break
        time.sleep(10)

# Rest of your imports remain the same

def read_query(table_name, query_file):
    with open(f"{table_name}/sql/{query_file}", 'r') as file:
        query = file.read()
    query_with_date = query.replace("{date}", current_date_str)
    logging.debug(f"read_query {query_file} with resulting query: {query_with_date} ")
    return query_with_date
def read_table(table_name, table_file):
    with open(f"{table_name}/tables/{table_file}", 'r') as file:
        query = file.read()
    query_with_date = query.replace("{date}", current_date_str)
    logging.debug(f"read_table {table_file} with resulting table: {query_with_date} ")
    return query_with_date

def read_response(table_name, result_file):
    # Ensure the file has a .json extension
    result_file_json = f"{os.path.splitext(result_file)[0]}.json"

    with open(f"{table_name}/results/{result_file_json}", 'r') as file:
        expected_result = json.load(file)

    # Define a lambda that captures expected_result and returns it when called
    response_lambda = lambda response : {
        'status': expected_result['data']['resp']['status'],
        'schema': expected_result['data']['resp']['schema'],
        'datarows': expected_result['data']['resp']['datarows'],
        'total': expected_result['data']['resp']['total'],
        'size': expected_result['data']['resp']['size']
    }

    # Log the lambda and its result for debugging
    logging.debug(f"expected response {expected_result} ")

    # Return the lambda function
    return response_lambda


def main(use_date, run_tables, run_queries):    # Default to current date if no argument is provided
    current_date_str = get_current_date_str()

    # Check if a date argument has been provided
    if use_date:
        # Use the provided date instead of the current date
        provided_date_str = use_date
        try:
            current_date_str = provided_date_str
        except ValueError as e:
            logging.error(f"Date argument provided is not in the correct format: {provided_date_str}")
            logging.error(f"Error: {e}")
            sys.exit(1)  # Exit the script if the date format is incorrect
    pass

    logging.info(f"Running tests for the date: {current_date_str}")
    logging.info(f"Enabling REPLE...")
    enable_repl()
    logging.info(f"Creating Session...")
    sessionId = create_session()

    logging.info(f"Starting Tests ...")
    # Iterate over tables files
    if run_tables:
        logging.info(f"Starting Create Table Tests ...")
        tables_files = os.listdir(f"{table_name}/tables")
        for table_file in tables_files:
            query = read_table(table_name, table_file)
            expected_lambda = lambda response: (
                    response['size'] == 0
            )
            test_repl(expected_lambda, query, sessionId)
    pass

    # Iterate over SQL query files
    if run_queries:
        logging.info(f"Starting SQL Queries Tests ...")
        query_files = os.listdir(f"{table_name}/sql")
        for query_file in query_files:
            query = read_query(table_name, query_file)
            expected_result = read_response(table_name, query_file)
            test_repl(expected_result, query, sessionId)

        # Iterate over PPL query files
        # logging.info(f"Starting PPL Queries Tests ...")
        # query_files = os.listdir(f"{table_name}/ppl")
        # for query_file in query_files:
        #     query = read_query(table_name, query_file)
        #     expected_result = read_response(table_name, query_file)
        #     test_repl(expected_result, query, sessionId)
    pass

    print_sanity_report()

def remove_field_identifiers(plan):
    # Regular expression to find field identifiers (e.g., "day#8")
    pattern = re.compile(r'\b(\w+)#\d+\b')
    # Replace field identifiers with just the field name
    modified_plan = pattern.sub(r'\1', plan)
    return modified_plan

if __name__ == '__main__':
    # Initialize the argument parser
    parser = argparse.ArgumentParser(description='Run table queries and/or other queries.')

    # Add arguments
    parser.add_argument('--use-date', type=str, help='Specify a date for the tables in YYYYMMDD format', default='')
    parser.add_argument('--run-tables', action='store_true', help='Run table queries')
    parser.add_argument('--run-queries', action='store_true', help='Run other queries')

    # Parse the arguments
    args = parser.parse_args()

    # Call main with the parsed arguments
    main(use_date=args.use_date ,run_tables=args.run_tables, run_queries=args.run_queries)



