SELECT count(*) as count, clientip
FROM mys3.default.http_logs_plain
WHERE clientip BETWEEN '208.0.0.0' AND '210.0.0.0'
GROUP BY clientip
ORDER BY count DESC
    limit 20;