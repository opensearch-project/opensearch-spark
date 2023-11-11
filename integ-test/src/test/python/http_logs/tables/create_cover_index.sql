CREATE INDEX status_clientip_and_day
    ON mys3.default.http_logs ( status, day, clientip )
    WITH (
  auto_refresh = true,
  refresh_interval = '5 minute',
  checkpoint_location = 's3://flint/-data/-dp/-eu/-west/-1/-beta/data/http_log/checkpoint_status_and_day'
)