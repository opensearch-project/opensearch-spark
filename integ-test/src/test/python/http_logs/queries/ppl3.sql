source = mys3.default.http_logs_plain | 
where status = 200 | stats count(status) by clientip, status |
sort - clientip | head 10