FROM bitnami/spark:3.5.3

# Install wget
USER root
RUN apt-get update && apt-get install -y wget && rm -rf /var/lib/apt/lists/*

# Define the Iceberg version and Maven repository URL
ENV ICEBERG_VERSION=1.5.0
ENV MAVEN_REPO=https://repo1.maven.org/maven2

# Download the Iceberg runtime JAR
RUN wget $MAVEN_REPO/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/$ICEBERG_VERSION/iceberg-spark-runtime-3.5_2.12-$ICEBERG_VERSION.jar \
    -O /opt/bitnami/spark/jars/iceberg-spark-runtime-3.5.jar

# Optional: Add configuration files
COPY spark-defaults.conf /opt/bitnami/spark/conf/

# Set up environment variables for Spark
ENV SPARK_MODE=master
ENV SPARK_RPC_AUTHENTICATION_ENABLED=no
ENV SPARK_RPC_ENCRYPTION_ENABLED=no
ENV SPARK_LOCAL_STORAGE_ENCRYPTION_ENABLED=no
ENV SPARK_SSL_ENABLED=no

# Switch back to non-root user for security
USER 1001