SELECT COUNT(DISTINCT clientip) as unique_client_ips
FROM mys3.default.http_logs_{date};