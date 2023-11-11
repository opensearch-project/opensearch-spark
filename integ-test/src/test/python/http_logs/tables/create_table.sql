CREATE EXTERNAL TABLE mys3.default.http_logs (
   `@timestamp` TIMESTAMP,
    clientip STRING,
    request STRING, 
    status INT, 
    size INT, 
    year INT, 
    month INT, 
    day INT) 
USING json PARTITIONED BY(year, month, day) OPTIONS 
   (path 's3://flint/-data/-dp/-eu/-west/-1/-beta/data/http_log/http_logs_partitioned_json_bz2/', compression 'bzip2')