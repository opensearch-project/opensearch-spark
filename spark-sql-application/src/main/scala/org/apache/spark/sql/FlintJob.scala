/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

// defined in spark package so that I can use ThreadUtils
package org.apache.spark.sql

import java.util.Locale
import java.util.concurrent.atomic.AtomicInteger

import org.opensearch.client.{RequestOptions, RestHighLevelClient}
import org.opensearch.cluster.metadata.MappingMetadata
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.XContentType
import org.opensearch.flint.core.{FlintClient, FlintClientBuilder, FlintOptions}
import org.opensearch.flint.core.metadata.FlintMetadata
import org.opensearch.flint.core.metrics.MetricConstants
import org.opensearch.flint.core.metrics.MetricsUtil.registerGauge
import play.api.libs.json._

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.flint.config.FlintSparkConf
import org.apache.spark.sql.types.{StructField, _}

/**
 * Spark SQL Application entrypoint
 *
 * @param args
 *   (0) sql query
 * @param args
 *   (1) opensearch index name
 * @return
 *   write sql query result to given opensearch index
 */
object FlintJob extends Logging with FlintJobExecutor {
  def main(args: Array[String]): Unit = {
    val (queryOption, resultIndex) = args.length match {
      case 1 =>
        (None, args(0)) // Starting from OS 2.13, resultIndex is the only argument
      case 2 =>
        (
          Some(args(0)),
          args(1)
        ) // Before OS 2.13, there are two arguments, the second one is resultIndex
      case _ =>
        throw new IllegalArgumentException(
          "Unsupported number of arguments. Expected 1 or 2 arguments.")
    }

    val conf = createSparkConf()
    val jobType = conf.get("spark.flint.job.type", "batch")
    logInfo(s"""Job type is: ${jobType}""")
    conf.set(FlintSparkConf.JOB_TYPE.key, jobType)

    val dataSource = conf.get("spark.flint.datasource.name", "")
    val query = queryOption.getOrElse(conf.get(FlintSparkConf.QUERY.key, ""))
    if (query.isEmpty) {
      throw new IllegalArgumentException(s"Query undefined for the ${jobType} job.")
    }
    // https://github.com/opensearch-project/opensearch-spark/issues/138
    /*
     * To execute queries such as `CREATE SKIPPING INDEX ON my_glue1.default.http_logs_plain (`@timestamp` VALUE_SET) WITH (auto_refresh = true)`,
     * it's necessary to set `spark.sql.defaultCatalog=my_glue1`. This is because AWS Glue uses a single database (default) and table (http_logs_plain),
     * and we need to configure Spark to recognize `my_glue1` as a reference to AWS Glue's database and table.
     * By doing this, we effectively map `my_glue1` to AWS Glue, allowing Spark to resolve the database and table names correctly.
     * Without this setup, Spark would not recognize names in the format `my_glue1.default`.
     */
    conf.set("spark.sql.defaultCatalog", dataSource)
    val streamingRunningCount = new AtomicInteger(0)
    val jobOperator =
      JobOperator(
        createSparkSession(conf),
        query,
        dataSource,
        resultIndex,
        jobType.equalsIgnoreCase("streaming"),
        streamingRunningCount)
    registerGauge(MetricConstants.STREAMING_RUNNING_METRIC, streamingRunningCount)
    jobOperator.start()
  }
}
