# Dockerfile
FROM --platform=linux/amd64 public.ecr.aws/emr-serverless/spark/emr-7.2.0:latest


# MODIFICATIONS GO HERE

# FROM 489478819445.dkr.ecr.us-west-2.amazonaws.com/amazoncorretto:8
# This is the official Correto 8 image location: https://docs.aws.amazon.com/corretto/latest/corretto-8-ug/docker-install.html
# For using EMR with dependencies via Docker image, any Docker image used with Spark must have Java installed in the Docker image
# https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-spark-docker.html#emr-spark-docker-ECR

USER root

#RUN yum install -y wget
#RUN wget -O /usr/local/bin/spark-submit-flamegraph \
#  https://raw.githubusercontent.com/spektom/spark-flamegraph/master/spark-submit-flamegraph
#RUN chmod +x /usr/local/bin/spark-submit-flamegraph

# copy the required depandancies to the classpath location
COPY ./lib/ /usr/lib/spark/jars/
COPY ./metrics.properties /etc/spark/conf
#COPY ./root-ca.pem /tmp

#RUN yes | keytool -import -alias opensearch -keystore $JAVA_HOME/lib/security/cacerts -file /tmp/root-ca.pem -storepass changeit

# EMRS will run the image as hadoop
USER hadoop:hadoop
