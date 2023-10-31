import requests
import time
import json
import os
import logging

url = os.environ.get('OPENSEARCH_URL', "http://opensearch_server:9200")  # Modify this line
logging.basicConfig(filename='sanity_report.log', level=logging.INFO)

test_result = []
sanity_report = []

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
        time.sleep(5)

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
        time.sleep(2)



def main():
    enable_repl()
    sessionId = create_session()

    expected_lambda = lambda response: (
            response['status'] == 'SUCCESS' and
            response['total'] == 1 and
            response['datarows'][0] == [1998] and
            response['schema'][0]['name'] == 'year' and
            response['schema'][0]['type'] == 'integer'
    )
    test_repl(expected_lambda, "select year from mys3.default.http_logs where year = 1998 limit 1", sessionId)


    expected_lambda = lambda response: (
            response['size'] == 13 and
            response['total'] == 13 and
            response['datarows'][0] == [
                "@timestamp",
                "timestamp",
                ""
            ] and
            response['schema'] == [
                {
                    "name": "col_name",
                    "type": "string"
                },
                {
                    "name": "data_type",
                    "type": "string"
                },
                {
                    "name": "comment",
                    "type": "string"
                }
            ]
    )
    test_repl(expected_lambda, "DESC mys3.default.http_logs", sessionId)

    expected_lambda = lambda response: (
            response['size'] == 1 and
            response['total'] == 1 and
            response['datarows'][0] == [
                "default",
                "http_logs",
                False
            ] and
            response['schema'] == [
                {"name": "namespace", "type": "string"},
                {"name": "tableName", "type": "string"},
                {"name": "isTemporary", "type": "boolean"}
            ]
    )
    test_repl(expected_lambda, "SHOW TABLES IN mys3.default LIKE 'http_logs'", sessionId)


    expected_lambda = lambda response: (
            response['size'] == 0
    )
    test_repl(expected_lambda, "create skipping index on mys3.default.http_logs (status VALUE_SET)", sessionId)
    print_sanity_report()

if __name__ == '__main__':
    main()    