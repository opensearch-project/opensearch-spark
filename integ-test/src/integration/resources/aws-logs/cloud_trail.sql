CREATE TABLE {table_name} (
    eventVersion STRING,
    userIdentity STRUCT<
      type:STRING,
      principalId:STRING,
      arn:STRING,
      accountId:STRING,
      invokedBy:STRING,
      accessKeyId:STRING,
      userName:STRING,
      sessionContext:STRUCT<
        attributes:STRUCT<
          mfaAuthenticated:STRING,
          creationDate:STRING
        >,
        sessionIssuer:STRUCT<
          type:STRING,
          principalId:STRING,
          arn:STRING,
          accountId:STRING,
          userName:STRING
        >,
        ec2RoleDelivery:STRING,
        webIdFederationData:MAP<STRING,STRING>
      >
    >,
    eventTime STRING,
    eventSource STRING,
    eventName STRING,
    awsRegion STRING,
    sourceIPAddress STRING,
    userAgent STRING,
    errorCode STRING,
    errorMessage STRING,
    requestParameters STRING,
    responseElements STRING,
    additionalEventData STRING,
    requestId STRING,
    eventId STRING,
    resources ARRAY<STRUCT<
      arn:STRING,
      accountId:STRING,
      type:STRING
    >>,
    eventType STRING,
    apiVersion STRING,
    readOnly STRING,
    recipientAccountId STRING,
    serviceEventDetails STRING,
    sharedEventId STRING,
    vpcEndpointId STRING,
    eventCategory STRING,
    tlsDetails STRUCT<
      tlsVersion:STRING,
      cipherSuite:STRING,
      clientProvidedHostHeader:STRING
    >
)
USING json
OPTIONS (
   recursivefilelookup='true',
   multiline 'true'
);

INSERT INTO {table_name} VALUES
(
    '1.08',
    NAMED_STRUCT(
        'type', 'IAMUser',
        'principalId', 'AWS123456789012',
        'arn', 'arn:aws:iam::123456789012:user/ExampleUser',
        'accountId', '123456789012',
        'invokedBy', null,
        'accessKeyId', 'AKIA1234567890',
        'userName', 'ExampleUser',
        'sessionContext', NAMED_STRUCT(
            'attributes', NAMED_STRUCT(
                'mfaAuthenticated', 'true',
                'creationDate', '2023-11-01T05:00:00Z'
            ),
            'sessionIssuer', NAMED_STRUCT(
                'type', 'Role',
                'principalId', 'ARO123456789012',
                'arn', 'arn:aws:iam::123456789012:role/MyRole',
                'accountId', '123456789012',
                'userName', 'MyRole'
            ),
            'ec2RoleDelivery', 'true',
            'webIdFederationData', MAP()
        )
    ),
    '2023-11-01T05:00:00Z',
    'sts.amazonaws.com',
    'AssumeRole',
    'us-east-1',
    '198.51.100.45',
    'AWS CLI',
    null,
    null,
    null,
    null,
    null,
    'request-id-1',
    'event-id-1',
    ARRAY(NAMED_STRUCT(
        'arn', 'arn:aws:iam::123456789012:role/MyRole',
        'accountId', '123456789012',
        'type', 'AWS::IAM::Role'
    )),
    'AwsApiCall',
    '2015-03-31',
    'true',
    '123456789012',
    null,
    null,
    null,
    'Management',
    NAMED_STRUCT(
        'tlsVersion', 'TLSv1.2',
        'cipherSuite', 'ECDHE-RSA-AES128-GCM-SHA256',
        'clientProvidedHostHeader', null
    )
),
(
    '1.08',
    NAMED_STRUCT(
        'type', 'IAMUser',
        'principalId', 'AWS123456789012',
        'arn', 'arn:aws:iam::123456789012:user/ExampleUser',
        'accountId', '123456789012',
        'invokedBy', null,
        'accessKeyId', 'AKIA1234567890',
        'userName', 'ExampleUser',
        'sessionContext', NAMED_STRUCT(
            'attributes', NAMED_STRUCT(
                'mfaAuthenticated', 'true',
                'creationDate', '2023-11-01T05:06:00Z'
            ),
            'sessionIssuer', NAMED_STRUCT(
                'type', 'Role',
                'principalId', 'ARO123456789012',
                'arn', 'arn:aws:iam::123456789012:role/MyRole',
                'accountId', '123456789012',
                'userName', 'MyRole'
            ),
            'ec2RoleDelivery', 'true',
            'webIdFederationData', MAP()
        )
    ),
    '2023-11-01T05:06:00Z',
    'sts.amazonaws.com',
    'AssumeRole',
    'us-east-1',
    '198.51.100.45',
    'AWS CLI',
    null,
    null,
    null,
    null,
    null,
    'request-id-2',
    'event-id-2',
    ARRAY(NAMED_STRUCT(
        'arn', 'arn:aws:iam::123456789012:role/MyRole',
        'accountId', '123456789012',
        'type', 'AWS::IAM::Role'
    )),
    'AwsApiCall',
    '2015-03-31',
    'true',
    '123456789012',
    null,
    null,
    null,
    'Management',
    NAMED_STRUCT(
        'tlsVersion', 'TLSv1.2',
        'cipherSuite', 'ECDHE-RSA-AES128-GCM-SHA256',
        'clientProvidedHostHeader', null
    )
);
