# Copyright OpenSearch Contributors
# SPDX-License-Identifier: Apache-2.0

FROM opensearchproject/opensearch:latest

USER root

RUN mkdir /tmp/alter-emr-jar
WORKDIR /tmp/alter-emr-jar

ENV AWS_VERSION=1.12.651

RUN curl -O -L https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-emrserverless/${AWS_VERSION}/aws-java-sdk-emrserverless-${AWS_VERSION}.jar
RUN curl -O -L https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-core/${AWS_VERSION}/aws-java-sdk-core-${AWS_VERSION}.jar

COPY emr-src /tmp/alter-emr-jar/emr-src
WORKDIR /tmp/alter-emr-jar/emr-src
RUN /usr/share/opensearch/jdk/bin/javac -cp ../aws-java-sdk-emrserverless-${AWS_VERSION}.jar:../aws-java-sdk-core-${AWS_VERSION}.jar com/amazonaws/services/emrserverless/AWSEMRServerlessClientBuilder.java org/opensearch/spark/emrserverless/DockerEMRServerlessClient.java
RUN mkdir /tmp/alter-emr-jar/extracted
WORKDIR /tmp/alter-emr-jar/extracted
RUN /usr/share/opensearch/jdk/bin/jar -xf ../aws-java-sdk-emrserverless-${AWS_VERSION}.jar
RUN cp ../emr-src/com/amazonaws/services/emrserverless/AWSEMRServerlessClientBuilder.class com/amazonaws/services/emrserverless/
RUN mkdir -p org/opensearch/spark/emrserverless
RUN cp ../emr-src/org/opensearch/spark/emrserverless/DockerEMRServerlessClient.class org/opensearch/spark/emrserverless/
RUN /usr/share/opensearch/jdk/bin/jar -cfM /usr/share/opensearch/plugins/opensearch-sql/aws-java-sdk-emrserverless-*.jar META-INF/MANIFEST.MF *
RUN chown opensearch:opensearch /usr/share/opensearch/plugins/opensearch-sql/aws-java-sdk-emrserverless-*.jar
RUN rm -rf /tmp/alter-emr-jar

RUN yum install -y docker util-linux

COPY opensearch-docker-it-entrypoint.sh /usr/share/opensearch/opensearch-docker-it-entrypoint.sh
COPY docker-command-runner.sh /usr/share/opensearch/docker-command-runner.sh
COPY opensearch_security.policy /usr/share/opensearch/config/opensearch-performance-analyzer/opensearch_security.policy
COPY log4j2.properties /usr/share/opensearch/config/log4j2.properties

RUN chown opensearch:opensearch /usr/share/opensearch/config/opensearch-performance-analyzer/opensearch_security.policy
RUN chown opensearch:opensearch /usr/share/opensearch/config/log4j2.properties

WORKDIR /usr/share/opensearch
ENTRYPOINT ["./opensearch-docker-it-entrypoint.sh"]
CMD ["opensearch"]

EXPOSE 9200
EXPOSE 9300
