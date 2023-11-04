Explain SELECT
            day,
            status
        FROM mys3.default.http_logs_plain
        WHERE status >= 400
        GROUP BY day, status
            LIMIT 100;
