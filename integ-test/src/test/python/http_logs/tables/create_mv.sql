CREATE MATERIALIZED VIEW mys3.default.http_count_view
AS
SELECT
    window.start AS `start.time`,
    COUNT(*) AS count
FROM mys3.default.http_logs
WHERE status != 200
GROUP BY TUMBLE(`@timestamp`, '1 Minutes')
WITH (
    auto_refresh = true,
    refresh_interval = '1 Minutes',
    checkpoint_location = 's3:/path/data/http_log/checkpoint_http_count_view',
    watermark_delay = '10 Minutes'
);