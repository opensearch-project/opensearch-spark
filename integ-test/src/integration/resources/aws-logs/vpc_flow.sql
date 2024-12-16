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
    (1, '123456789012', 'eni-abc123', '10.0.0.1', '10.0.0.2', 12345, 80, 6, 10, 200, 1698814800, 1698814860, 'ACCEPT', 'OK'),  -- 05:00:00 to 05:01:00 UTC
    (2, '123456789012', 'eni-def456', '10.0.0.1', '10.0.0.2', 12346, 443, 6, 5, 150, 1698814900, 1698814960, 'ACCEPT', 'OK'),  -- 05:01:40 to 05:02:40 UTC
    (3, '123456789013', 'eni-ghi789', '10.0.0.3', '10.0.0.4', 12347, 22, 6, 15, 300, 1698815400, 1698815460, 'ACCEPT', 'OK'),  -- 05:10:00 to 05:11:00 UTC
    (4, '123456789013', 'eni-jkl012', '10.0.0.5', '10.0.0.6', 12348, 21, 6, 20, 400, 1698815500, 1698815560, 'REJECT', 'OK'),  -- 05:11:40 to 05:12:40 UTC
    (5, '123456789014', 'eni-mno345', '10.0.0.7', '10.0.0.8', 12349, 25, 6, 25, 500, 1698816000, 1698816060, 'ACCEPT', 'OK')   -- 05:20:00 to 05:21:00 UTC
;
