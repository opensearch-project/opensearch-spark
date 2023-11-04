SELECT
    FIRST(day) AS day,
    status,
    COUNT(status) AS status_count_by_day
FROM mys3.default.http_logs_plain
WHERE status >= 400
GROUP BY day, status
ORDER BY day, status
    LIMIT 20;