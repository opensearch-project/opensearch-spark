/bin/sh

# Copyright OpenSearch Contributors
# SPDX-License-Identifier: Apache-2.0

# Login to Minio
curl -q \
     -c /tmp/minio-cookies.txt \
     -H 'Content-Type: application/json' \
     -d '{"accessKey": "minioadmin", "secretKey": "minioadmin"}' \
     http://minio-S3:9001/api/v1/login
# Delete the test bucket
curl -b /tmp/minio-cookies.txt \
     -X DELETE \
     http://minio-S3:9001/api/v1/buckets/test
# Create the integ-test bucket
curl -q \
     -b /tmp/minio-cookies.txt \
     -X POST \
     -H 'Content-Type: application/json' \
     -d '{"name": "integ-test", "versioning": {"enabled": true, "excludePrefixes": [], "excludeFolders": false}, "locking": true}' \
     http://minio-S3:9001/api/v1/buckets
# Create the test-resources bucket
curl -q \
     -b /tmp/minio-cookies.txt \
     -X POST \
     -H 'Content-Type: application/json' \
     -d '{"name": "test-resources", "versioning": {"enabled": false, "excludePrefixes": [], "excludeFolders": false}, "locking": true}' \
     http://minio-S3:9001/api/v1/buckets
# Create the access key
curl -q \
     -b /tmp/minio-cookies.txt \
     -X GET
     "http://minio-S3:9001/api/v1/service-accounts/${S3_ACCESS_KEY}"
if [ "$?" -ne "0" ]; then
  curl -q \
       -b /tmp/minio-cookies.txt \
       -X POST \
       -H 'Content-Type: application/json' \
       -d "{\"policy\": \"\", \"accessKey\": \"${S3_ACCESS_KEY}\", \"secretKey\": \"${S3_SECRET_KEY}\", \"description\": \"\", \"comment\": \"\", \"name\": \"\", \"expiry\": null}" \
       http://minio-S3:9001/api/v1/service-account-credentials
fi

# Login to OpenSearch Dashboards
echo ">>> Login to OpenSearch dashboards"
curl -q \
     -c /tmp/opensearch-cookies.txt \
     -X POST \
     -H 'Content-Type: application/json' \
     -H 'Osd-Version: 2.18.0' \
     -H 'Osd-Xsrf: fetch' \
     -d "{\"username\": \"admin\", \"password\": \"${OPENSEARCH_ADMIN_PASSWORD}\"}" \
     'http://opensearch-dashboards:5601/auth/login?dataSourceId='
if [ "$?" -eq "0" ]; then
  echo "    >>> Login successful"
else
  echo "    >>> Login failed"
fi

# Create the S3/Glue datasource
curl -q \
     -b /tmp/opensearch-cookies.txt \
     -X GET \
     http://localhost:5601/api/directquery/dataconnections/mys3
if [ "$?" -ne "0" ]; then
  echo ">>> Creating datasource"
  curl -q \
       -b /tmp/opensearch-cookies.txt \
       -X POST \
       -H 'Content-Type: application/json' \
       -H 'Osd-Version: 2.18.0' \
       -H 'Osd-Xsrf: fetch' \
       -d "{\"name\": \"mys3\", \"allowedRoles\": [], \"connector\": \"s3glue\", \"properties\": {\"glue.auth.type\": \"iam_role\", \"glue.auth.role_arn\": \"arn:aws:iam::123456789012:role/S3Access\", \"glue.indexstore.opensearch.uri\": \"http://opensearch:9200\", \"glue.indexstore.opensearch.auth\": \"basicauth\", \"glue.indexstore.opensearch.auth.username\": \"admin\", \"glue.indexstore.opensearch.auth.password\": \"${OPENSEARCH_ADMIN_PASSWORD}\"}}" \
       http://opensearch-dashboards:5601/api/directquery/dataconnections
  if [ "$?" -eq "0" ]; then
    echo "    >>> S3 datasource created"
  else
    echo "    >>> Failed to create S3 datasource"
  fi

  echo ">>> Setting cluster settings"
  curl -v \
       -u "admin:${OPENSEARCH_ADMIN_PASSWORD}" \
       -X PUT \
       -H 'Content-Type: application/json' \
       -d '{"persistent": {"plugins.query.executionengine.spark.config": "{\"applicationId\":\"integ-test\",\"executionRoleARN\":\"arn:aws:iam::xxxxx:role/emr-job-execution-role\",\"region\":\"us-west-2\", \"sparkSubmitParameters\": \"--conf spark.dynamicAllocation.enabled=false\"}"}}' \
       http://opensearch:9200/_cluster/settings
  if [ "$?" -eq "0" ]; then
    echo "    >>> Successfully set cluster settings"
  else
    echo "    >>> Failed to set cluster settings"
  fi
fi
