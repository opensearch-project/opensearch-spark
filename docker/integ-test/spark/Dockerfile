# Copyright OpenSearch Contributors
# SPDX-License-Identifier: Apache-2.0

ARG SPARK_VERSION=3.5.3
FROM bitnami/spark:${SPARK_VERSION}

USER root
RUN apt update
RUN apt install -y curl

USER 1001
COPY ./spark-defaults.conf /opt/bitnami/spark/conf/spark-defaults.conf
COPY ./log4j2.properties /opt/bitnami/spark/conf/log4j2.properties
COPY ./spark-master-entrypoint.sh /opt/bitnami/scripts/spark/spark-master-entrypoint.sh
