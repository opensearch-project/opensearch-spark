SELECT day, SUM(size) as total_size FROM mys3.default.http_logs
WHERE year = 1998 AND month =6
GROUP BY day;