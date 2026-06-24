# Copyright OpenSearch Contributors
# SPDX-License-Identifier: Apache-2.0

# The openjdk Docker Hub images are deprecated and 21-jdk-bookworm no longer resolves.
# eclipse-temurin:21-jdk-jammy is a maintained Debian-based JDK 21 drop-in.
FROM eclipse-temurin:21-jdk-jammy

WORKDIR /opt

ENV HADOOP_HOME=/opt/hadoop-3.3.4
ENV HIVE_HOME=/opt/apache-hive-2.3.9-bin

# eclipse-temurin does not ship curl (the old openjdk image did), so install it first.
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*
RUN curl -L https://archive.apache.org/dist/hive/hive-2.3.9/apache-hive-2.3.9-bin.tar.gz | tar zxf -
RUN curl -L https://archive.apache.org/dist/hadoop/common/hadoop-3.3.4/hadoop-3.3.4.tar.gz | tar zxf -
RUN cp $HADOOP_HOME/share/hadoop/client/hadoop-client-api-3.3.4.jar $HIVE_HOME/lib/
RUN cp $HADOOP_HOME/share/hadoop/client/hadoop-client-runtime-3.3.4.jar $HIVE_HOME/lib/
RUN cp $HADOOP_HOME/share/hadoop/tools/lib/hadoop-aws-3.3.4.jar $HIVE_HOME/lib/
RUN cp $HADOOP_HOME/share/hadoop/tools/lib/aws-java-sdk-bundle-1.12.262.jar $HIVE_HOME/lib/

RUN groupadd -f -r hive --gid=1000
RUN useradd -r -g hive --uid=1000 -d ${HIVE_HOME} hive
RUN chown hive:hive -R ${HIVE_HOME}

RUN mkdir /data
RUN chown hive:hive /data

WORKDIR $HIVE_HOME
EXPOSE 9083
ENTRYPOINT ["/opt/apache-hive-2.3.9-bin/bin/hive", "--service", "metastore"]
USER hive
