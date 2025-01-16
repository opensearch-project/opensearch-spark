# Copyright OpenSearch Contributors
# SPDX-License-Identifier: Apache-2.0

FROM bitnami/spark:3.5.3
ARG FLINT_JAR
ARG PPL_JAR
ARG SQL_APP_JAR
ENV FLINT_JAR $FLINT_JAR
ENV PPL_JAR $PPL_JAR
ENV SQL_APP_JAR $SQL_APP_JAR

COPY docker/integ-test/spark/spark-defaults.conf /opt/bitnami/spark/conf/spark-defaults.conf
COPY ${FLINT_JAR} /opt/bitnami/spark/jars/flint-spark-integration.jar
COPY ${PPL_JAR} /opt/bitnami/spark/jars/ppl-spark-integration.jar

USER root
RUN mkdir /app
COPY ${SQL_APP_JAR} /app/spark-sql-application.jar

USER 1001
