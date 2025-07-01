FROM sbtscala/scala-sbt:eclipse-temurin-11.0.17_8_1.8.1_2.12.17 AS builder

WORKDIR /build
COPY . .

# Disable tests to speed up assembly
RUN sed -i \
        -e 's/compileScalastyle := (Compile \/ scalastyle).toTask("").value/compileScalastyle := {}/g' \
        -e 's/assembly \/ test := (Test \/ test).value/assembly \/ test := {}/g' \
        build.sbt

RUN SBT_OPTS="-Xmx4G" sbt assembly

FROM bitnami/spark:3.5.6 AS spark

COPY --from=builder /build/ppl-spark-integration/target/scala-2.12/ppl-spark-integration-assembly-1.0.0-SNAPSHOT.jar /opt/bitnami/spark/jars/ppl-spark-integration.jar
COPY --from=builder /build/flint-spark-integration/target/scala-2.12/flint-spark-integration-assembly-1.0.0-SNAPSHOT.jar /opt/bitnami/spark/jars/flint-spark-integration.jar
COPY --from=builder /build/spark-sql-application/target/scala-2.12/sql-job-assembly-1.0.0-SNAPSHOT.jar /opt/bitnami/spark/jars/opensearch-spark-sql-application.jar
COPY docker/web/spark-defaults.conf /opt/bitnami/spark/conf/spark-defaults.conf

FROM spark AS web

RUN pip install --break-system-packages "pyspark==3.5.6" flask pandas

WORKDIR /app
COPY docker/web/server.py .

CMD ["python3", "-m", "flask", "--app", "server", "run", "--host=0.0.0.0"]
