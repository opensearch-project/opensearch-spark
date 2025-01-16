#!/bin/bash

# Copyright OpenSearch Contributors
# SPDX-License-Identifier: Apache-2.0

function start_spark_connect() {
  sc_version=$(ls -1 /opt/bitnami/spark/jars/spark-core_*.jar | sed -e 's/^.*\/spark-core_//' -e 's/\.jar$//' -e 's/-/:/')

  attempt=1
  while [ -e "/tmp/spark_master_running" -a "$attempt" -le 10 ]; do
    sleep 1
    /opt/bitnami/spark/sbin/start-connect-server.sh --master spark://spark:7077 --packages org.apache.spark:spark-connect_${sc_version}
    attempt=$(($attempt+1))
  done
}

touch /tmp/spark_master_running
start_spark_connect &
/opt/bitnami/scripts/spark/entrypoint.sh /opt/bitnami/scripts/spark/run.sh
rm /tmp/spark_master_running
