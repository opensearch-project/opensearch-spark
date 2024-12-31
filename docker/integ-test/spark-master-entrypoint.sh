#!/bin/bash

# Add passwd and shadow entries so that su works
grep -q '^spark:' /etc/passwd
if [ "$?" -ne "0" ]; then
  echo 'spark:x:1001:0:spark:/opt/bitnami/spark:/bin/bash' >> /etc/passwd
fi
grep -q '^spark:' /etc/shadow
if [ "$?" -ne "0" ]; then
  echo 'spark:*:17885:0:99999:7:::' >> /etc/shadow
fi

apt update
apt install -y curl

S3_ACCESS_KEY=`grep '^ACCESS_KEY=' /opt/bitnami/spark/s3.credentials | sed -e 's/^.*=//'`
S3_SECRET_KEY=`grep '^SECRET_KEY=' /opt/bitnami/spark/s3.credentials | sed -e 's/^.*=//'`

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
# Create the access key
curl -q \
     -b /tmp/minio-cookies.txt \
     -X POST \
     -H 'Content-Type: application/json' \
     -d "{\"policy\": \"\", \"accessKey\": \"${S3_ACCESS_KEY}\", \"secretKey\": \"${S3_SECRET_KEY}\", \"description\": \"\", \"comment\": \"\", \"name\": \"\", \"expiry\": null}" \
     http://minio-S3:9001/api/v1/service-account-credentials

# Login to OpenSearch Dashboards
curl -c /tmp/opensearch-cookies.txt \
     -X POST \
     -H 'Content-Type: application/json' \
     -H 'Osd-Version: 2.18.0' \
     -H 'Osd-Xsrf: fetch' \
     -d "{\"username\": \"admin\", \"password\": \"${OPENSEARCH_ADMIN_PASSWORD}\"}" \
     'http://opensearch-dashboards:5601/auth/login?dataSourceId='
# Create the S3/Glue datasource
curl -b /tmp/opensearch-cookies.txt \
     -X POST \
     -H 'Content-Type: application/json' \
     -H 'Osd-Version: 2.18.0' \
     -H 'Osd-Xsrf: fetch' \
     -d '{"name": "mys3", "allowedRoles": [], "connector": "s3glue", "properties": {"glue.auth.type": "iam_role", "glue.auth.role_arn": "arn:aws:iam::123456789012:role/S3Access", "glue.indexstore.opensearch.uri": "http://opensearch:9200", "glue.indexstore.opensearch.auth": "basicauth", "glue.indexstore.opensearch.auth.username": "admin", "glue.indexstore.opensearch.auth.password": "arn:aws:iam::123456789012:role/S3Access"}}' \
     http://opensearch-dashboards:5601/api/directquery/dataconnections

su spark -c '/opt/bitnami/scripts/spark/entrypoint.sh /opt/bitnami/scripts/spark/run.sh'
