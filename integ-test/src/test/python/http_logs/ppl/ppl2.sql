source = mys3.default.http_logs_plain | 
where status >= 400 | sort - @timestamp | head 5