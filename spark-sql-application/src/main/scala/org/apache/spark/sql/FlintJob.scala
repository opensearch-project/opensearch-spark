/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

// defined in spark package so that I can use ThreadUtils
package org.apache.spark.sql

import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.{ExecutionContext, Future, TimeoutException}
import scala.concurrent.duration._
import scala.util.control.NonFatal

import com.codahale.metrics.Timer
import org.opensearch.flint.common.model.FlintStatement
import org.opensearch.flint.core.logging.CustomLogging
import org.opensearch.flint.core.metrics.{MetricConstants, MetricsUtil}
import org.opensearch.flint.core.metrics.MetricsUtil.{getTimerContext, incrementCounter, registerGauge, stopTimer}

import org.apache.spark.SparkConf
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

  private val statementRunningCount = new AtomicInteger(0)

  def main(args: Array[String]): Unit = {
    val (queryOption, resultIndexOption) = parseArgs(args)

    val conf = createSparkConf()
    val sparkSession = createSparkSession(conf)
    val applicationId =
      environmentProvider.getEnvVar("SERVERLESS_EMR_VIRTUAL_CLUSTER_ID", "unknown")
    val jobId = environmentProvider.getEnvVar("SERVERLESS_EMR_JOB_ID", "unknown")
    val warmpoolEnabled = conf.get(FlintSparkConf.WARMPOOL_ENABLED.key, "false").toBoolean

    if (!warmpoolEnabled) {
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
      val resultIndex = resultIndexOption.get

      processStreamingJob(
        applicationId,
        jobId,
        query,
        queryId,
        dataSource,
        resultIndex,
        jobType,
        sparkSession,
        Map.empty)
      return
    }

    handleWarmpoolJob(applicationId, jobId, sparkSession, resultIndexOption)
  }

  private def handleWarmpoolJob(
      applicationId: String,
      jobId: String,
      sparkSession: SparkSession,
      resultIndexOption: Option[String]): Unit = {
    sparkSession.conf.set(
      FlintSparkConf.CUSTOM_SESSION_MANAGER.key,
      "com.amazon.client.WarmpoolSessionManagerDqsImpl")
    val sessionManager = instantiateSessionManager(sparkSession, resultIndexOption)

    var commandContext = CommandContext(
      applicationId,
      jobId,
      sparkSession,
      "", // In WP flow, FlintJob doesn't know the dataSource
      "", // In WP flow, FlintJob doesn't know the jobType
      "", // FlintJob doesn't have sessionId
      sessionManager, // FlintJob doesn't have SessionManager
      Duration.Inf, // FlintJob doesn't have queryExecutionTimeout
      -1, // FlintJob doesn't have inactivityLimitMillis
      -1, // FlintJob doesn't have queryWaitTimeMillis
      -1 // FlintJob doesn't have queryLoopExecutionFrequency
    )
    val segmentName = sparkSession.conf.get("spark.dynamicAllocation.maxExecutors")
    registerGauge(
      String.format("%se.%s", segmentName, MetricConstants.STATEMENT_RUNNING_METRIC),
      statementRunningCount)
    val statementExecutionManager = instantiateStatementExecutionManager(commandContext)
    var counter = 0

    while (counter < 2) {
      try {
        counter = counter + 1;
        statementExecutionManager.getNextStatement() match {
          case Some(flintStatement) =>
            logInfo(s"flintStatement received: ${flintStatement}")
            val jobType = flintStatement.context.get("jobType") match {
              case Some(s: String) => s
              case _ => ""
            }
            val dataSource = flintStatement.context.get("dataSource") match {
              case Some(s: String) => s
              case _ => ""
            }
            var resultIndex = flintStatement.context.get("resultIndex") match {
              case Some(s: String) => s
              case _ => ""
            }

            val host = flintStatement.context.get("host") match {
              case Some(s: String) => s
              case _ => ""
            }

            val queryId = flintStatement.queryId
            val query = flintStatement.query

            logInfo(s"""JobType: ${jobType}""")
            logInfo(s"""dataSource: ${dataSource}""")
            logInfo(s"""resultIndex: ${resultIndex}""")
            if (jobType.isEmpty || dataSource.isEmpty || resultIndex.isEmpty) {
              logInfo("jobType, dataSource, or resultIndex is not set")
            }

            CustomLogging.logInfo(s"""Job type is: ${jobType}""")
            sparkSession.conf.set(FlintSparkConf.JOB_TYPE.key, jobType)
            sparkSession.conf.set(FlintSparkConf.DATA_SOURCE_NAME.key, dataSource)
            logInfo(
              s"Job Type from sparkConf: ${sparkSession.conf.get(FlintSparkConf.JOB_TYPE.key)}")
            logInfo(
              s"Datasource from sparkConf: ${sparkSession.conf.get(FlintSparkConf.DATA_SOURCE_NAME.key)}")

            if (!dataSource.contains("_CWLBasic")) {
              sparkSession.conf.set(
                FlintSparkConf.CUSTOM_FLINT_METADATA_LOG_SERVICE_CLASS.key,
                "com.amazon.client.FlintMetadataLogServiceDqsImpl")
              sparkSession.conf.set(
                FlintSparkConf.CUSTOM_FLINT_SCHEDULER_CLASS.key,
                "com.amazon.client.AsyncQuerySchedulerDqsImpl")
              sparkSession.conf.set(
                FlintSparkConf.CUSTOM_FLINT_INDEX_METADATA_SERVICE_CLASS.key,
                "com.amazon.client.FlintIndexMetadataServiceDqsImpl")
              sparkSession.conf.set(FlintSparkConf.HOST_ENDPOINT.key, host)
              sparkSession.conf.set(
                "spark.datasource.flint.customAWSCredentialsProvider",
                "com.amazon.fireflower.FireFlowerDataSourceAccessCredentialsProvider")
            } else {
              sparkSession.conf.set(FlintSparkConf.HOST_ENDPOINT.key, "localhost")
              sparkSession.conf.set(
                "spark.datasource.flint.customAWSCredentialsProvider",
                "com.amazonaws.emr.AssumeRoleAWSCredentialsProvider")
              sparkSession.conf.set("spark.flint.datasource.name", "_CWLBasic")
            }

            if (jobType.equalsIgnoreCase(FlintJobType.STREAMING) || jobType.equalsIgnoreCase(
                FlintJobType.BATCH)) {
              processStreamingJob(
                applicationId,
                jobId,
                query,
                queryId,
                dataSource,
                resultIndex,
                jobType,
                sparkSession,
                flintStatement.context)
            } else {
              handleInteractiveJob(
                sparkSession,
                commandContext,
                flintStatement,
                statementExecutionManager,
                applicationId,
                jobId,
                dataSource,
                segmentName)
            }
        }
      } catch {
        case e: Throwable =>
          CustomLogging.logInfo(s"""Execution failed with: ${e.printStackTrace()}""")
      }
    }

    // Check for non-daemon threads that may prevent the driver from shutting down.
    // Non-daemon threads other than the main thread indicate that the driver is still processing tasks,
    // which may be due to unresolved bugs in dependencies or threads not being properly shut down.
    if (terminateJVM && threadPoolFactory.hasNonDaemonThreadsOtherThanMain) {
      logInfo("A non-daemon thread in the driver is seen.")
      // Exit the JVM to prevent resource leaks and potential emr-s job hung.
      // A zero status code is used for a graceful shutdown without indicating an error.
      // If exiting with non-zero status, emr-s job will fail.
      // This is a part of the fault tolerance mechanism to handle such scenarios gracefully
      System.exit(0)
    }
    sparkSession.stop()
  }

  private def handleInteractiveJob(
      sparkSession: SparkSession,
      commandContext: CommandContext,
      flintStatement: FlintStatement,
      statementExecutionManager: StatementExecutionManager,
      applicationId: String,
      jobId: String,
      dataSource: String,
      segmentName: String): Unit = {
    implicit val ec: ExecutionContext = ExecutionContext.global
    var dataToWrite: Option[DataFrame] = None
    val startTime: Long = currentTimeProvider.currentEpochMillis()
    flintStatement.running()
    statementExecutionManager.updateStatement(flintStatement)
    statementRunningCount.incrementAndGet()
    val statementTimerContext = getTimerContext(MetricConstants.STATEMENT_PROCESSING_TIME_METRIC)

    val futurePrepareQueryExecution = Future {
      statementExecutionManager.prepareStatementExecution()
    }

    val queryResultWriter = instantiateQueryResultWriter(sparkSession, commandContext)
    try {
      val df = statementExecutionManager.executeStatement(flintStatement)

      dataToWrite = Some(
        ThreadUtils.awaitResult(futurePrepareQueryExecution, Duration(1, MINUTES)) match {
          case Right(_) => queryResultWriter.processDataFrame(df, flintStatement, startTime)
          case Left(error) =>
            handleCommandFailureAndGetFailedData(
              applicationId,
              jobId,
              sparkSession,
              dataSource,
              error,
              flintStatement,
              "",
              startTime)
        })
    } catch {
      case e: TimeoutException =>
        incrementCounter(
          String.format("%se.%s", segmentName, MetricConstants.STATEMENT_EXECUTION_FAILED_METRIC))
        val error = s"Query execution preparation timed out"
        CustomLogging.logError(error, e)
        dataToWrite = Some(
          handleCommandTimeout(
            applicationId,
            jobId,
            sparkSession,
            dataSource,
            error,
            flintStatement,
            "",
            startTime))
      case NonFatal(e) =>
        incrementCounter(
          String.format("%se.%s", segmentName, MetricConstants.STATEMENT_EXECUTION_FAILED_METRIC))
        val error = s"An unexpected error occurred: ${e.getMessage}"
        CustomLogging.logError(error, e)
        dataToWrite = Some(
          handleCommandFailureAndGetFailedData(
            applicationId,
            jobId,
            sparkSession,
            dataSource,
            error,
            flintStatement,
            "",
            startTime))
    } finally {
      emitStatementQueryExecutionTimeMetric(startTime, segmentName)
      val resultWriterStartTime: Long = currentTimeProvider.currentEpochMillis()
      try {
        dataToWrite.foreach(df => queryResultWriter.writeDataFrame(df, flintStatement))
        if (flintStatement.isRunning || flintStatement.isWaiting) {
          flintStatement.complete()
        }
      } catch {
        case e: Exception =>
          incrementCounter(String
            .format("%se.%s", segmentName, MetricConstants.STATEMENT_RESULT_WRITER_FAILED_METRIC))
          val error = s"""Fail to write result of ${flintStatement}, cause: ${e.getMessage}"""
          CustomLogging.logError(error, e)
          flintStatement.fail()
      } finally {
        emitStatementResultWriterTimeMetric(resultWriterStartTime, segmentName)
        emitStatementTotalTimeMetric(startTime, segmentName)
        statementExecutionManager.updateStatement(flintStatement)
        recordStatementStateChange(
          statementRunningCount,
          flintStatement,
          statementTimerContext,
          segmentName)
      }
      statementExecutionManager.terminateStatementExecution()
    }
  }

  private def emitStatementQueryExecutionTimeMetric(
      startTime: Long,
      segmentName: String): Unit = {
    MetricsUtil
      .addHistoricGauge(
        String.format(
          "%se.%s",
          segmentName,
          MetricConstants.STATEMENT_QUERY_EXECUTION_TIME_METRIC),
        System.currentTimeMillis() - startTime)
  }

  private def emitStatementResultWriterTimeMetric(startTime: Long, segmentName: String): Unit = {
    MetricsUtil
      .addHistoricGauge(
        String.format("%se.%s", segmentName, MetricConstants.STATEMENT_RESULT_WRITER_TIME_METRIC),
        System.currentTimeMillis() - startTime)
  }

  private def emitStatementTotalTimeMetric(startTime: Long, segmentName: String): Unit = {
    MetricsUtil
      .addHistoricGauge(
        String.format("%se.%s", segmentName, MetricConstants.STATEMENT_QUERY_TOTAL_TIME_METRIC),
        System.currentTimeMillis() - startTime)
  }

  private def processStreamingJob(
      applicationId: String,
      jobId: String,
      query: String,
      queryId: String,
      dataSource: String,
      resultIndex: String,
      jobType: String,
      sparkSession: SparkSession,
      statementContext: Map[String, Any]): Unit = {
    // https://github.com/opensearch-project/opensearch-spark/issues/138
    /*
     * To execute queries such as `CREATE SKIPPING INDEX ON my_glue1.default.http_logs_plain (`@timestamp` VALUE_SET) WITH (auto_refresh = true)`,
     * it's necessary to set `spark.sql.defaultCatalog=my_glue1`. This is because AWS Glue uses a single database (default) and table (http_logs_plain),
     * and we need to configure Spark to recognize `my_glue1` as a reference to AWS Glue's database and table.
     * By doing this, we effectively map `my_glue1` to AWS Glue, allowing Spark to resolve the database and table names correctly.
     * Without this setup, Spark would not recognize names in the format `my_glue1.default`.
     */
    sparkSession.conf.set("spark.sql.defaultCatalog", dataSource)
    val segmentName = sparkSession.conf.get("spark.dynamicAllocation.maxExecutors")

//    sparkSession.conf.set(
//      "spark.dynamicAllocation.maxExecutors",
//      sparkSession.conf
//        .get("spark.flint.streaming.dynamicAllocation.maxExecutors", "10"))

    val streamingRunningCount = new AtomicInteger(0)
    val jobOperator =
      JobOperator(
        applicationId,
        jobId,
        sparkSession,
        query,
        queryId,
        dataSource,
        resultIndex,
        jobType,
        streamingRunningCount,
        statementContext)
    registerGauge(
      String.format("%se.%s", segmentName, MetricConstants.STREAMING_RUNNING_METRIC),
      streamingRunningCount)
    jobOperator.start()
  }

  private def instantiateStatementExecutionManager(
      commandContext: CommandContext): StatementExecutionManager = {
    import commandContext._
    instantiate(
      new StatementExecutionManagerImpl(commandContext),
      spark.conf.get(FlintSparkConf.CUSTOM_STATEMENT_MANAGER.key, ""),
      spark,
      "dummySessionId")
  }

  private def instantiateSessionManager(
      sparkSession: SparkSession,
      resultIndexOption: Option[String]): SessionManager = {
    instantiate(
      new SessionManagerImpl(sparkSession, Some("something")),
      sparkSession.conf.get(FlintSparkConf.CUSTOM_SESSION_MANAGER.key, ""),
      resultIndexOption.getOrElse(""))
  }

  private def handleCommandTimeout(
      applicationId: String,
      jobId: String,
      spark: SparkSession,
      dataSource: String,
      error: String,
      flintStatement: FlintStatement,
      sessionId: String,
      startTime: Long) = {
    spark.sparkContext.cancelJobGroup(flintStatement.queryId)
    flintStatement.timeout()
    flintStatement.error = Some(error)
    super.constructErrorDF(
      applicationId,
      jobId,
      spark,
      dataSource,
      flintStatement.state,
      error,
      flintStatement.queryId,
      flintStatement.query,
      sessionId,
      startTime)
  }

  def handleCommandFailureAndGetFailedData(
      applicationId: String,
      jobId: String,
      spark: SparkSession,
      dataSource: String,
      error: String,
      flintStatement: FlintStatement,
      sessionId: String,
      startTime: Long): DataFrame = {
    flintStatement.fail()
    flintStatement.error = Some(error)
    super.constructErrorDF(
      applicationId,
      jobId,
      spark,
      dataSource,
      flintStatement.state,
      error,
      flintStatement.queryId,
      flintStatement.query,
      sessionId,
      startTime)
  }

  private def recordStatementStateChange(
      statementRunningCount: AtomicInteger,
      flintStatement: FlintStatement,
      statementTimerContext: Timer.Context,
      segmentName: String): Unit = {
    stopTimer(statementTimerContext)
    if (statementRunningCount.get() > 0) {
      statementRunningCount.decrementAndGet()
    }
    if (flintStatement.isComplete) {
      incrementCounter(
        String.format("%se.%s", segmentName, MetricConstants.STATEMENT_SUCCESS_METRIC))
    } else if (flintStatement.isFailed) {
      incrementCounter(
        String.format("%se.%s", segmentName, MetricConstants.STATEMENT_FAILED_METRIC))
    }
  }

  private def instantiateQueryResultWriter(
      spark: SparkSession,
      commandContext: CommandContext): QueryResultWriter = {

    instantiate(
      new QueryResultWriterImpl(commandContext),
      spark.conf.get(FlintSparkConf.CUSTOM_QUERY_RESULT_WRITER.key, ""))
  }
}
