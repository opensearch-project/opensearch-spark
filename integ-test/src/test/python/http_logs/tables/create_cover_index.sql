CREATE INDEX status_clientip_and_day_{date}
    ON mys3.default.http_logs_{date} ( status, day, clientip )
    WITH (
  auto_refresh = true,
  refresh_interval = '5 minute',
  checkpoint_location = 's3://path/data/http_log/checkpoint_status_and_day'
)