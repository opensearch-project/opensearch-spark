CREATE TABLE {table_name} (
  version INT,
  accountId STRING,
  interfaceId STRING,
  srcAddr STRING,
  dstAddr STRING,
  srcPort INT,
  dstPort INT,
  protocol BIGINT,
  packets BIGINT,
  bytes BIGINT,
  start BIGINT,
  `end` BIGINT,
  action STRING,
  logStatus STRING
)
USING csv
OPTIONS (
  sep = ' ',
  recursiveFileLookup = 'true'
);

INSERT INTO {table_name} VALUES
    (1, '123456789012', 'eni-abc123', '10.0.0.1', '10.0.0.2', 12345, 80, 6, 10, 200, 1622548800, 1622548860, 'ACCEPT', 'OK'),  -- 5:00:00 AM to 5:01:00 AM PDT
    (2, '123456789012', 'eni-def456', '10.0.0.1', '10.0.0.2', 12346, 443, 6, 5, 150, 1622548900, 1622548960, 'ACCEPT', 'OK'),  -- 5:01:40 AM to 5:02:40 AM PDT
    (3, '123456789013', 'eni-ghi789', '10.0.0.3', '10.0.0.4', 12347, 22, 6, 15, 300, 1622549400, 1622549460, 'ACCEPT', 'OK'),  -- 5:10:00 AM to 5:11:00 AM PDT
    (4, '123456789013', 'eni-jkl012', '10.0.0.5', '10.0.0.6', 12348, 21, 6, 20, 400, 1622549500, 1622549560, 'REJECT', 'OK'),  -- 5:11:40 AM to 5:12:40 AM PDT
    (5, '123456789014', 'eni-mno345', '10.0.0.7', '10.0.0.8', 12349, 25, 6, 25, 500, 1622550000, 1622550060, 'ACCEPT', 'OK')   -- 5:20:00 AM to 5:21:00 AM PDT
;
