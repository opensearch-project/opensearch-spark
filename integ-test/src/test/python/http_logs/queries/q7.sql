SELECT count(*) as count, clientip
FROM mys3.default.http_logs_{date}
WHERE clientip BETWEEN '208.0.0.0' AND '210.0.0.0'
GROUP BY clientip
ORDER BY DESC count
limit 20;