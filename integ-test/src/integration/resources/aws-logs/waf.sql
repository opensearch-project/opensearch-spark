CREATE TABLE {table_name} (
    timestamp STRING,
    webaclId STRING,
    action STRING,
    formatVersion INT,
    httpRequest STRUCT<
        clientIp: STRING,
        country: STRING,
        headers: ARRAY<STRUCT<
            name: STRING,
            value: STRING
        >>,
        uri: STRING,
        args: STRING,
        httpVersion: STRING,
        httpMethod: STRING,
        requestId: STRING
    >,
    httpSourceId STRING,
    httpSourceName STRING,
    requestBodySize INT,
    requestBodySizeInspectedByWAF INT,
    terminatingRuleId STRING,
    terminatingRuleType STRING,
    ruleGroupList ARRAY<STRUCT<
        ruleId: STRING,
        ruleAction: STRING
    >>,
    rateBasedRuleList ARRAY<STRUCT<
        ruleId: STRING
    >>,
    nonTerminatingMatchingRules ARRAY<STRUCT<
        ruleId: STRING
    >>
)
USING json
OPTIONS (
    recursivefilelookup = 'true'
);

INSERT INTO {table_name} VALUES
(
    1698814800000,  -- 2023-11-01T05:00:00Z
    'webacl-12345',
    'ALLOW',
    1,
    NAMED_STRUCT(
        'clientIp', '192.0.2.1',
        'country', 'US',
        'headers', ARRAY(NAMED_STRUCT('name', 'User-Agent', 'value', 'Mozilla/5.0')),
        'uri', '/index.html',
        'args', 'query=example',
        'httpVersion', 'HTTP/1.1',
        'httpMethod', 'GET',
        'requestId', 'req-1'
    ),
    'source-1',
    'http-source',
    500,
    450,
    'rule-1',
    'REGULAR',
    ARRAY(NAMED_STRUCT('ruleId', 'group-rule-1', 'ruleAction', 'ALLOW')),
    ARRAY(),
    ARRAY()
),
(
    1698815400000,  -- 2023-11-01T05:10:00Z
    'webacl-67890',
    'BLOCK',
    1,
    NAMED_STRUCT(
        'clientIp', '192.0.2.2',
        'country', 'CA',
        'headers', ARRAY(NAMED_STRUCT('name', 'Referer', 'value', 'example.com')),
        'uri', '/login.html',
        'args', '',
        'httpVersion', 'HTTP/2',
        'httpMethod', 'POST',
        'requestId', 'req-2'
    ),
    'source-2',
    'http-source',
    750,
    600,
    'rule-2',
    'RATE_BASED',
    ARRAY(NAMED_STRUCT('ruleId', 'group-rule-2', 'ruleAction', 'BLOCK')),
    ARRAY(),
    ARRAY()
);
