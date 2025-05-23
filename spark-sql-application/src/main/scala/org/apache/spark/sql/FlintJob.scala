/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

// defined in spark package so that I can use ThreadUtils
package org.apache.spark.sql

import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}

import org.opensearch.flint.common.model.FlintStatement
import org.opensearch.flint.common.scheduler.model.LangType
import org.opensearch.flint.core.logging.CustomLogging
import org.opensearch.flint.core.metrics.MetricConstants
import org.opensearch.flint.core.metrics.MetricsUtil.registerGauge

import org.apache.spark.internal.Logging
import org.apache.spark.sql.flint.config.FlintSparkConf
import org.apache.spark.util.ThreadUtils

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
  private val streamingRunningCount = new AtomicInteger(0)
  private val statementRunningCount = new AtomicInteger(0)

  def main(args: Array[String]): Unit = {
    val (queryOption, resultIndexOption) = parseArgs(args)

    val conf = createSparkConf()
    val sparkSession = createSparkSession(conf)
    val applicationId =
      environmentProvider.getEnvVar("SERVERLESS_EMR_VIRTUAL_CLUSTER_ID", "unknown")
    val jobId = environmentProvider.getEnvVar("SERVERLESS_EMR_JOB_ID", "unknown")
    val isWarmpoolEnabled = conf.get(FlintSparkConf.WARMPOOL_ENABLED.key, "false").toBoolean
    logInfo(s"isWarmpoolEnabled: ${isWarmpoolEnabled}")

    if (!isWarmpoolEnabled) {
      val jobType = sparkSession.conf.get("spark.flint.job.type", FlintJobType.BATCH)
      CustomLogging.logInfo(s"""Job type is: ${jobType}""")
      sparkSession.conf.set(FlintSparkConf.JOB_TYPE.key, jobType)

      val dataSource = conf.get("spark.flint.datasource.name", "")
      val query = queryOption.getOrElse(unescapeQuery(conf.get(FlintSparkConf.QUERY.key, "")))
      if (query.isEmpty) {
        logAndThrow(s"Query undefined for the ${jobType} job.")
      }
      val queryId = conf.get(FlintSparkConf.QUERY_ID.key, "")

      if (resultIndexOption.isEmpty) {
        logAndThrow("resultIndex is not set")
      }

      configDYNMaxExecutors(conf, jobType)
      val segmentName = sparkSession.conf.get("spark.dynamicAllocation.maxExecutors")
      val flintStatement =
        new FlintStatement(
          "running",
          query,
          "",
          queryId,
          LangType.SQL,
          currentTimeProvider.currentEpochMillis(),
          Option.empty,
          Map.empty)

      val jobOperator = createJobOperator(
        sparkSession,
        applicationId,
        jobId,
        flintStatement,
        dataSource,
        resultIndexOption.get,
        jobType,
        streamingRunningCount,
        statementRunningCount)

      registerGauge(MetricConstants.STREAMING_RUNNING_METRIC, streamingRunningCount)
      jobOperator.start()
    } else {
      // Fetch and execute queries in warm pool mode
      val warmpoolJob =
        WarmpoolJob(
          applicationId,
          jobId,
          sparkSession,
          streamingRunningCount,
          statementRunningCount)
      warmpoolJob.start()
    }
  }
}
