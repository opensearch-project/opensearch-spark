#!/bin/bash

# Copyright OpenSearch Contributors
# SPDX-License-Identifier: Apache-2.0

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

su spark -c '/opt/bitnami/scripts/spark/entrypoint.sh /opt/bitnami/scripts/spark/run.sh'
