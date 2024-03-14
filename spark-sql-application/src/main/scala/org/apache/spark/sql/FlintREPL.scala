/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql

import java.net.ConnectException
import java.util.concurrent.{ScheduledExecutorService, ScheduledFuture}
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future, TimeoutException}
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

import com.codahale.metrics.Timer
import org.json4s.native.Serialization
import org.opensearch.action.get.GetResponse
import org.opensearch.common.Strings
import org.opensearch.flint.app.{FlintCommand, FlintInstance}
import org.opensearch.flint.app.FlintInstance.formats
import org.opensearch.flint.core.FlintOptions
import org.opensearch.flint.core.metrics.MetricConstants
import org.opensearch.flint.core.metrics.MetricsUtil.{decrementCounter, getTimerContext, incrementCounter, registerGauge, stopTimer}
import org.opensearch.flint.core.storage.{FlintReader, OpenSearchUpdater}
import org.opensearch.search.sort.SortOrder

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.FlintJob.createSparkSession
import org.apache.spark.sql.flint.config.FlintSparkConf
import org.apache.spark.sql.flint.config.FlintSparkConf.REPL_INACTIVITY_TIMEOUT_MILLIS
import org.apache.spark.sql.util.{DefaultShutdownHookManager, ShutdownHookManagerTrait}
import org.apache.spark.util.ThreadUtils

/**
 * Spark SQL Application entrypoint
 *
 * @param args(0)
 *   sql query
 * @param args(1)
 *   opensearch result index name
 * @param args(2)
 *   opensearch connection values required for flint-integration jar. host, port, scheme, auth,
 *   region respectively.
 * @return
 *   write sql query result to given opensearch index
 */
object FlintREPL extends Logging with FlintJobExecutor {

  private val HEARTBEAT_INTERVAL_MILLIS = 60000L
  private val MAPPING_CHECK_TIMEOUT = Duration(1, MINUTES)
  private val DEFAULT_QUERY_EXECUTION_TIMEOUT = Duration(30, MINUTES)
  private val DEFAULT_QUERY_WAIT_TIMEOUT_MILLIS = 10 * 60 * 1000
  val INITIAL_DELAY_MILLIS = 3000L
  val EARLY_TERMIANTION_CHECK_FREQUENCY = 60000L

  @volatile var earlyExitFlag: Boolean = false
  // termiante JVM in the presence non-deamon thread before exiting
  var terminateJVM = true

  def updateSessionIndex(flintCommand: FlintCommand, updater: OpenSearchUpdater): Unit = {
    updater.update(flintCommand.statementId, FlintCommand.serialize(flintCommand))
  }

  private val sessionRunningCount = new AtomicInteger(0)
  private val statementRunningCount = new AtomicInteger(0)

  def main(args: Array[String]) {
    val (queryOption, resultIndex) = parseArgs(args)

    if (Strings.isNullOrEmpty(resultIndex)) {
      throw new IllegalArgumentException("resultIndex is not set")
    }

    // init SparkContext
    val conf: SparkConf = createSparkConf()
    val dataSource = conf.get(FlintSparkConf.DATA_SOURCE_NAME.key, "unknown")
    // https://github.com/opensearch-project/opensearch-spark/issues/138
    /*
     * To execute queries such as `CREATE SKIPPING INDEX ON my_glue1.default.http_logs_plain (`@timestamp` VALUE_SET) WITH (auto_refresh = true)`,
     * it's necessary to set `spark.sql.defaultCatalog=my_glue1`. This is because AWS Glue uses a single database (default) and table (http_logs_plain),
     * and we need to configure Spark to recognize `my_glue1` as a reference to AWS Glue's database and table.
     * By doing this, we effectively map `my_glue1` to AWS Glue, allowing Spark to resolve the database and table names correctly.
     * Without this setup, Spark would not recognize names in the format `my_glue1.default`.
     */
    conf.set("spark.sql.defaultCatalog", dataSource)

    val jobType = conf.get(FlintSparkConf.JOB_TYPE.key, FlintSparkConf.JOB_TYPE.defaultValue.get)
    logInfo(s"""Job type is: ${FlintSparkConf.JOB_TYPE.defaultValue.get}""")
    conf.set(FlintSparkConf.JOB_TYPE.key, jobType)

    val query = getQuery(queryOption, jobType, conf)

    if (jobType.equalsIgnoreCase("streaming")) {
      logInfo(s"""streaming query ${query}""")
      val streamingRunningCount = new AtomicInteger(0)
      val jobOperator =
        JobOperator(
          createSparkSession(conf),
          query,
          dataSource,
          resultIndex,
          true,
          streamingRunningCount)
      registerGauge(MetricConstants.STREAMING_RUNNING_METRIC, streamingRunningCount)
      jobOperator.start()
    } else {
      // we don't allow default value for sessionIndex and sessionId. Throw exception if key not found.
      val sessionIndex: Option[String] = Option(conf.get(FlintSparkConf.REQUEST_INDEX.key, null))
      val sessionId: Option[String] = Option(conf.get(FlintSparkConf.SESSION_ID.key, null))

      if (sessionIndex.isEmpty) {
        throw new IllegalArgumentException(FlintSparkConf.REQUEST_INDEX.key + " is not set")
      }
      if (sessionId.isEmpty) {
        throw new IllegalArgumentException(FlintSparkConf.SESSION_ID.key + " is not set")
      }

      val spark = createSparkSession(conf)
      val osClient = new OSClient(FlintSparkConf().flintOptions())
      val jobId = envinromentProvider.getEnvVar("SERVERLESS_EMR_JOB_ID", "unknown")
      val applicationId =
        envinromentProvider.getEnvVar("SERVERLESS_EMR_VIRTUAL_CLUSTER_ID", "unknown")

      // Read the values from the Spark configuration or fall back to the default values
      val inactivityLimitMillis: Long =
        conf.getLong(
          FlintSparkConf.REPL_INACTIVITY_TIMEOUT_MILLIS.key,
          FlintOptions.DEFAULT_INACTIVITY_LIMIT_MILLIS)
      val queryExecutionTimeoutSecs: Duration = Duration(
        conf.getLong(
          "spark.flint.job.queryExecutionTimeoutSec",
          DEFAULT_QUERY_EXECUTION_TIMEOUT.toSeconds),
        SECONDS)
      val queryWaitTimeoutMillis: Long =
        conf.getLong("spark.flint.job.queryWaitTimeoutMillis", DEFAULT_QUERY_WAIT_TIMEOUT_MILLIS)

      val flintSessionIndexUpdater = osClient.createUpdater(sessionIndex.get)
      val sessionTimerContext = getTimerContext(MetricConstants.REPL_PROCESSING_TIME_METRIC)

      addShutdownHook(
        flintSessionIndexUpdater,
        osClient,
        sessionIndex.get,
        sessionId.get,
        sessionTimerContext)

      // 1 thread for updating heart beat
      val threadPool =
        threadPoolFactory.newDaemonThreadPoolScheduledExecutor("flint-repl-heartbeat", 1)

      registerGauge(MetricConstants.REPL_RUNNING_METRIC, sessionRunningCount)
      registerGauge(MetricConstants.STATEMENT_RUNNING_METRIC, statementRunningCount)
      val jobStartTime = currentTimeProvider.currentEpochMillis()
      // update heart beat every 30 seconds
      // OpenSearch triggers recovery after 1 minute outdated heart beat
      var heartBeatFuture: ScheduledFuture[_] = null
      try {
        heartBeatFuture = createHeartBeatUpdater(
          HEARTBEAT_INTERVAL_MILLIS,
          flintSessionIndexUpdater,
          sessionId.get,
          threadPool,
          osClient,
          sessionIndex.get,
          INITIAL_DELAY_MILLIS)

        if (setupFlintJobWithExclusionCheck(
            conf,
            sessionIndex,
            sessionId,
            osClient,
            jobId,
            applicationId,
            flintSessionIndexUpdater,
            jobStartTime)) {
          earlyExitFlag = true
          return
        }

        val commandContext = CommandContext(
          spark,
          dataSource,
          resultIndex,
          sessionId.get,
          flintSessionIndexUpdater,
          osClient,
          sessionIndex.get,
          jobId,
          queryExecutionTimeoutSecs,
          inactivityLimitMillis,
          queryWaitTimeoutMillis)
        exponentialBackoffRetry(maxRetries = 5, initialDelay = 2.seconds) {
          queryLoop(commandContext)
        }
        recordSessionSuccess(sessionTimerContext)
      } catch {
        case e: Exception =>
          handleSessionError(
            e,
            applicationId,
            jobId,
            sessionId.get,
            jobStartTime,
            flintSessionIndexUpdater,
            osClient,
            sessionIndex.get,
            sessionTimerContext)
      } finally {
        if (threadPool != null) {
          heartBeatFuture.cancel(true) // Pass `true` to interrupt if running
          threadPoolFactory.shutdownThreadPool(threadPool)
        }
        stopTimer(sessionTimerContext)
        spark.stop()

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
      }
    }
  }

  def parseArgs(args: Array[String]): (Option[String], String) = {
    args.length match {
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
  }

  def getQuery(queryOption: Option[String], jobType: String, conf: SparkConf): String = {
    queryOption.getOrElse {
      if (jobType.equalsIgnoreCase("streaming")) {
        val defaultQuery = conf.get(FlintSparkConf.QUERY.key, "")
        if (defaultQuery.isEmpty) {
          throw new IllegalArgumentException("Query undefined for the streaming job.")
        }
        defaultQuery
      } else ""
    }
  }

  /**
   * Sets up a Flint job with exclusion checks based on the job configuration.
   *
   * This method will first check if there are any jobs to exclude from execution based on the
   * configuration provided. If the current job's ID is in the exclusion list, the method will
   * signal to exit early to avoid redundant execution. This is also true if the job is identified
   * as a duplicate of a currently running job.
   *
   * If there are no conflicts with excluded job IDs or duplicate jobs, the method proceeds to set
   * up the Flint job as normal.
   *
   * @param conf
   *   A SparkConf object containing the job's configuration.
   * @param sessionIndex
   *   The index within OpenSearch where session information is stored.
   * @param sessionId
   *   The current session's ID.
   * @param osClient
   *   The OpenSearch client used to interact with OpenSearch.
   * @param jobId
   *   The ID of the current job.
   * @param applicationId
   *   The application ID for the current Flint session.
   * @param flintSessionIndexUpdater
   *   An OpenSearch updater for Flint session indices.
   * @param jobStartTime
   *   The start time of the job.
   * @return
   *   A Boolean value indicating whether to exit the job early (true) or not (false).
   * @note
   *   If the sessionIndex or sessionId Options are empty, the method will throw a
   *   NoSuchElementException, as `.get` is called on these options without checking for their
   *   presence.
   */
  def setupFlintJobWithExclusionCheck(
      conf: SparkConf,
      sessionIndex: Option[String],
      sessionId: Option[String],
      osClient: OSClient,
      jobId: String,
      applicationId: String,
      flintSessionIndexUpdater: OpenSearchUpdater,
      jobStartTime: Long): Boolean = {
    val confExcludeJobsOpt = conf.getOption(FlintSparkConf.EXCLUDE_JOB_IDS.key)

    confExcludeJobsOpt match {
      case None =>
        // If confExcludeJobs is None, pass null or an empty sequence as per your setupFlintJob method's signature
        setupFlintJob(
          applicationId,
          jobId,
          sessionId.get,
          flintSessionIndexUpdater,
          sessionIndex.get,
          jobStartTime)

      case Some(confExcludeJobs) =>
        // example: --conf spark.flint.deployment.excludeJobs=job-1,job-2
        val excludeJobIds = confExcludeJobs.split(",").toList // Convert Array to Lis

        if (excludeJobIds.contains(jobId)) {
          logInfo(s"current job is excluded, exit the application.")
          return true
        }

        val getResponse = osClient.getDoc(sessionIndex.get, sessionId.get)
        if (getResponse.isExists()) {
          val source = getResponse.getSourceAsMap
          if (source != null) {
            val existingExcludedJobIds = parseExcludedJobIds(source)
            if (excludeJobIds.sorted == existingExcludedJobIds.sorted) {
              logInfo("duplicate job running, exit the application.")
              return true
            }
          }
        }

        // If none of the edge cases are met, proceed with setup
        setupFlintJob(
          applicationId,
          jobId,
          sessionId.get,
          flintSessionIndexUpdater,
          sessionIndex.get,
          jobStartTime,
          excludeJobIds)
    }
    false
  }

  def queryLoop(commandContext: CommandContext): Unit = {
    // 1 thread for updating heart beat
    val threadPool = threadPoolFactory.newDaemonThreadPoolScheduledExecutor("flint-repl-query", 1)
    implicit val executionContext = ExecutionContext.fromExecutor(threadPool)

    var futureMappingCheck: Future[Either[String, Unit]] = null
    try {
      futureMappingCheck = Future {
        checkAndCreateIndex(commandContext.osClient, commandContext.resultIndex)
      }

      var lastActivityTime = currentTimeProvider.currentEpochMillis()
      var verificationResult: VerificationResult = NotVerified
      var canPickUpNextStatement = true
      var lastCanPickCheckTime = 0L
      while (currentTimeProvider
          .currentEpochMillis() - lastActivityTime <= commandContext.inactivityLimitMillis && canPickUpNextStatement) {
        logInfo(
          s"""read from ${commandContext.sessionIndex}, sessionId: ${commandContext.sessionId}""")
        val flintReader: FlintReader =
          createQueryReader(
            commandContext.osClient,
            commandContext.sessionId,
            commandContext.sessionIndex,
            commandContext.dataSource)

        try {
          val commandState = CommandState(
            lastActivityTime,
            verificationResult,
            flintReader,
            futureMappingCheck,
            executionContext,
            lastCanPickCheckTime)
          val result: (Long, VerificationResult, Boolean, Long) =
            processCommands(commandContext, commandState)

          val (
            updatedLastActivityTime,
            updatedVerificationResult,
            updatedCanPickUpNextStatement,
            updatedLastCanPickCheckTime) = result

          lastActivityTime = updatedLastActivityTime
          verificationResult = updatedVerificationResult
          canPickUpNextStatement = updatedCanPickUpNextStatement
          lastCanPickCheckTime = updatedLastCanPickCheckTime
        } finally {
          flintReader.close()
        }

        Thread.sleep(100)
      }
    } finally {
      if (threadPool != null) {
        threadPoolFactory.shutdownThreadPool(threadPool)
      }
    }
  }

  private def setupFlintJob(
      applicationId: String,
      jobId: String,
      sessionId: String,
      flintSessionIndexUpdater: OpenSearchUpdater,
      sessionIndex: String,
      jobStartTime: Long,
      excludeJobIds: Seq[String] = Seq.empty[String]): Unit = {
    val includeJobId = !excludeJobIds.isEmpty && !excludeJobIds.contains(jobId)
    val currentTime = currentTimeProvider.currentEpochMillis()
    val flintJob = new FlintInstance(
      applicationId,
      jobId,
      sessionId,
      "running",
      currentTime,
      jobStartTime,
      excludeJobIds)

    val serializedFlintInstance = if (includeJobId) {
      FlintInstance.serialize(flintJob, currentTime, true)
    } else {
      FlintInstance.serializeWithoutJobId(flintJob, currentTime)
    }
    flintSessionIndexUpdater.upsert(sessionId, serializedFlintInstance)
    logInfo(
      s"""Updated job: {"jobid": ${flintJob.jobId}, "sessionId": ${flintJob.sessionId}} from $sessionIndex""")
    sessionRunningCount.incrementAndGet()
  }

  def handleSessionError(
      e: Exception,
      applicationId: String,
      jobId: String,
      sessionId: String,
      jobStartTime: Long,
      flintSessionIndexUpdater: OpenSearchUpdater,
      osClient: OSClient,
      sessionIndex: String,
      sessionTimerContext: Timer.Context): Unit = {
    val error = s"Session error: ${e.getMessage}"
    logError(error, e)

    val flintInstance = getExistingFlintInstance(osClient, sessionIndex, sessionId)
      .getOrElse(createFailedFlintInstance(applicationId, jobId, sessionId, jobStartTime, error))

    updateFlintInstance(flintInstance, flintSessionIndexUpdater, sessionId)
    if (flintInstance.state.equals("fail")) {
      recordSessionFailed(sessionTimerContext)
    }
  }

  private def getExistingFlintInstance(
      osClient: OSClient,
      sessionIndex: String,
      sessionId: String): Option[FlintInstance] = Try(
    osClient.getDoc(sessionIndex, sessionId)) match {
    case Success(getResponse) if getResponse.isExists() =>
      Option(getResponse.getSourceAsMap)
        .map(FlintInstance.deserializeFromMap)
    case Failure(exception) =>
      logError(s"Failed to retrieve existing FlintInstance: ${exception.getMessage}", exception)
      None
    case _ => None
  }

  private def createFailedFlintInstance(
      applicationId: String,
      jobId: String,
      sessionId: String,
      jobStartTime: Long,
      errorMessage: String): FlintInstance = new FlintInstance(
    applicationId,
    jobId,
    sessionId,
    "fail",
    currentTimeProvider.currentEpochMillis(),
    jobStartTime,
    error = Some(errorMessage))

  private def updateFlintInstance(
      flintInstance: FlintInstance,
      flintSessionIndexUpdater: OpenSearchUpdater,
      sessionId: String): Unit = {
    val currentTime = currentTimeProvider.currentEpochMillis()
    flintSessionIndexUpdater.upsert(
      sessionId,
      FlintInstance.serializeWithoutJobId(flintInstance, currentTime))
  }

  /**
   * handling the case where a command's execution fails, updates the flintCommand with the error
   * and failure status, and then write the result to result index. Thus, an error is written to
   * both result index or statement model in request index
   *
   * @param spark
   *   spark session
   * @param dataSource
   *   data source
   * @param error
   *   error message
   * @param flintCommand
   *   flint command
   * @param sessionId
   *   session id
   * @param startTime
   *   start time
   * @return
   *   failed data frame
   */
  def handleCommandFailureAndGetFailedData(
      spark: SparkSession,
      dataSource: String,
      error: String,
      flintCommand: FlintCommand,
      sessionId: String,
      startTime: Long): DataFrame = {
    flintCommand.fail()
    flintCommand.error = Some(error)
    super.getFailedData(
      spark,
      dataSource,
      error,
      flintCommand.queryId,
      flintCommand.query,
      sessionId,
      startTime,
      currentTimeProvider)
  }

  def processQueryException(
      ex: Exception,
      spark: SparkSession,
      dataSource: String,
      flintCommand: FlintCommand,
      sessionId: String): String = {
    val error = super.processQueryException(
      ex,
      spark,
      dataSource,
      flintCommand.query,
      flintCommand.queryId,
      sessionId)
    flintCommand.fail()
    flintCommand.error = Some(error)
    error
  }

  private def processCommands(
      context: CommandContext,
      state: CommandState): (Long, VerificationResult, Boolean, Long) = {
    import context._
    import state._

    var lastActivityTime = recordedLastActivityTime
    var verificationResult = recordedVerificationResult
    var canProceed = true
    var canPickNextStatementResult = true // Add this line to keep track of canPickNextStatement
    var lastCanPickCheckTime = recordedLastCanPickCheckTime

    while (canProceed) {
      val currentTime = currentTimeProvider.currentEpochMillis()

      // Only call canPickNextStatement if EARLY_TERMIANTION_CHECK_FREQUENCY milliseconds have passed
      if (currentTime - lastCanPickCheckTime > EARLY_TERMIANTION_CHECK_FREQUENCY) {
        canPickNextStatementResult =
          canPickNextStatement(sessionId, jobId, osClient, sessionIndex)
        lastCanPickCheckTime = currentTime
      }

      if (!canPickNextStatementResult) {
        earlyExitFlag = true
        canProceed = false
      } else if (!flintReader.hasNext) {
        canProceed = false
      } else {
        val statementTimerContext = getTimerContext(
          MetricConstants.STATEMENT_PROCESSING_TIME_METRIC)
        val flintCommand = processCommandInitiation(flintReader, flintSessionIndexUpdater)

        val (dataToWrite, returnedVerificationResult) = processStatementOnVerification(
          recordedVerificationResult,
          spark,
          flintCommand,
          dataSource,
          sessionId,
          executionContext,
          futureMappingCheck,
          resultIndex,
          queryExecutionTimeout,
          queryWaitTimeMillis)

        verificationResult = returnedVerificationResult
        finalizeCommand(
          dataToWrite,
          flintCommand,
          resultIndex,
          flintSessionIndexUpdater,
          osClient,
          statementTimerContext)
        // last query finish time is last activity time
        lastActivityTime = currentTimeProvider.currentEpochMillis()
      }
    }

    // return tuple indicating if still active and mapping verification result
    (lastActivityTime, verificationResult, canPickNextStatementResult, lastCanPickCheckTime)
  }

  /**
   * finalize command after processing
   *
   * @param dataToWrite
   *   data to write
   * @param flintCommand
   *   flint command
   * @param resultIndex
   *   result index
   * @param flintSessionIndexUpdater
   *   flint session index updater
   */
  private def finalizeCommand(
      dataToWrite: Option[DataFrame],
      flintCommand: FlintCommand,
      resultIndex: String,
      flintSessionIndexUpdater: OpenSearchUpdater,
      osClient: OSClient,
      statementTimerContext: Timer.Context): Unit = {
    try {
      dataToWrite.foreach(df => writeDataFrameToOpensearch(df, resultIndex, osClient))
      if (flintCommand.isRunning() || flintCommand.isWaiting()) {
        // we have set failed state in exception handling
        flintCommand.complete()
      }
      updateSessionIndex(flintCommand, flintSessionIndexUpdater)
      recordStatementStateChange(flintCommand, statementTimerContext)
    } catch {
      // e.g., maybe due to authentication service connection issue
      // or invalid catalog (e.g., we are operating on data not defined in provided data source)
      case e: Exception =>
        val error = s"""Fail to write result of ${flintCommand}, cause: ${e.getMessage}"""
        logError(error, e)
        flintCommand.fail()
        updateSessionIndex(flintCommand, flintSessionIndexUpdater)
        recordStatementStateChange(flintCommand, statementTimerContext)
    }
  }

  private def handleCommandTimeout(
      spark: SparkSession,
      dataSource: String,
      error: String,
      flintCommand: FlintCommand,
      sessionId: String,
      startTime: Long): Option[DataFrame] = {
    /*
     * https://tinyurl.com/2ezs5xj9
     *
     * This only interrupts active Spark jobs that are actively running.
     * This would then throw the error from ExecutePlan and terminate it.
     * But if the query is not running a Spark job, but executing code on Spark driver, this
     * would be a noop and the execution will keep running.
     *
     * In Apache Spark, actions that trigger a distributed computation can lead to the creation
     * of Spark jobs. In the context of Spark SQL, this typically happens when we perform
     * actions that require the computation of results that need to be collected or stored.
     */
    spark.sparkContext.cancelJobGroup(flintCommand.queryId)
    logError(error)
    Some(
      handleCommandFailureAndGetFailedData(
        spark,
        dataSource,
        error,
        flintCommand,
        sessionId,
        startTime))
  }

  def executeAndHandle(
      spark: SparkSession,
      flintCommand: FlintCommand,
      dataSource: String,
      sessionId: String,
      executionContext: ExecutionContextExecutor,
      startTime: Long,
      queryExecuitonTimeOut: Duration,
      queryWaitTimeMillis: Long): Option[DataFrame] = {
    try {
      Some(
        executeQueryAsync(
          spark,
          flintCommand,
          dataSource,
          sessionId,
          executionContext,
          startTime,
          queryExecuitonTimeOut,
          queryWaitTimeMillis))
    } catch {
      case e: TimeoutException =>
        handleCommandTimeout(
          spark,
          dataSource,
          s"Executing ${flintCommand.query} timed out",
          flintCommand,
          sessionId,
          startTime)
      case e: Exception =>
        val error = processQueryException(e, spark, dataSource, flintCommand.query, "", "")
        Some(
          handleCommandFailureAndGetFailedData(
            spark,
            dataSource,
            error,
            flintCommand,
            sessionId,
            startTime))
    }
  }

  private def processStatementOnVerification(
      recordedVerificationResult: VerificationResult,
      spark: SparkSession,
      flintCommand: FlintCommand,
      dataSource: String,
      sessionId: String,
      executionContext: ExecutionContextExecutor,
      futureMappingCheck: Future[Either[String, Unit]],
      resultIndex: String,
      queryExecutionTimeout: Duration,
      queryWaitTimeMillis: Long): (Option[DataFrame], VerificationResult) = {
    val startTime: Long = currentTimeProvider.currentEpochMillis()
    var verificationResult = recordedVerificationResult
    var dataToWrite: Option[DataFrame] = None

    verificationResult match {
      case NotVerified =>
        try {
          ThreadUtils.awaitResult(futureMappingCheck, MAPPING_CHECK_TIMEOUT) match {
            case Right(_) =>
              dataToWrite = executeAndHandle(
                spark,
                flintCommand,
                dataSource,
                sessionId,
                executionContext,
                startTime,
                queryExecutionTimeout,
                queryWaitTimeMillis)
              verificationResult = VerifiedWithoutError
            case Left(error) =>
              verificationResult = VerifiedWithError(error)
              dataToWrite = Some(
                handleCommandFailureAndGetFailedData(
                  spark,
                  dataSource,
                  error,
                  flintCommand,
                  sessionId,
                  startTime))
          }
        } catch {
          case e: TimeoutException =>
            val error = s"Getting the mapping of index $resultIndex timed out"
            dataToWrite =
              handleCommandTimeout(spark, dataSource, error, flintCommand, sessionId, startTime)
          case NonFatal(e) =>
            val error = s"An unexpected error occurred: ${e.getMessage}"
            dataToWrite = Some(
              handleCommandFailureAndGetFailedData(
                spark,
                dataSource,
                error,
                flintCommand,
                sessionId,
                startTime))
        }
      case VerifiedWithError(err) =>
        dataToWrite = Some(
          handleCommandFailureAndGetFailedData(
            spark,
            dataSource,
            err,
            flintCommand,
            sessionId,
            startTime))
      case VerifiedWithoutError =>
        dataToWrite = executeAndHandle(
          spark,
          flintCommand,
          dataSource,
          sessionId,
          executionContext,
          startTime,
          queryExecutionTimeout,
          queryWaitTimeMillis)
    }

    logInfo(s"command complete: $flintCommand")
    (dataToWrite, verificationResult)
  }

  def executeQueryAsync(
      spark: SparkSession,
      flintCommand: FlintCommand,
      dataSource: String,
      sessionId: String,
      executionContext: ExecutionContextExecutor,
      startTime: Long,
      queryExecutionTimeOut: Duration,
      queryWaitTimeMillis: Long): DataFrame = {
    if (currentTimeProvider
        .currentEpochMillis() - flintCommand.submitTime > queryWaitTimeMillis) {
      handleCommandFailureAndGetFailedData(
        spark,
        dataSource,
        "wait timeout",
        flintCommand,
        sessionId,
        startTime)
    } else {
      val futureQueryExecution = Future {
        executeQuery(spark, flintCommand.query, dataSource, flintCommand.queryId, sessionId)
      }(executionContext)

      // time out after 10 minutes
      ThreadUtils.awaitResult(futureQueryExecution, queryExecutionTimeOut)
    }
  }
  private def processCommandInitiation(
      flintReader: FlintReader,
      flintSessionIndexUpdater: OpenSearchUpdater): FlintCommand = {
    val command = flintReader.next()
    logDebug(s"raw command: $command")
    val flintCommand = FlintCommand.deserialize(command)
    logDebug(s"command: $flintCommand")
    flintCommand.running()
    logDebug(s"command running: $flintCommand")
    updateSessionIndex(flintCommand, flintSessionIndexUpdater)
    statementRunningCount.incrementAndGet()
    flintCommand
  }

  private def createQueryReader(
      osClient: OSClient,
      sessionId: String,
      sessionIndex: String,
      dataSource: String) = {
    // all state in index are in lower case
    // we only search for statement submitted in the last hour in case of unexpected bugs causing infinite loop in the
    // same doc
    val dsl =
      s"""{
         |  "bool": {
         |    "must": [
         |    {
         |        "term": {
         |          "type": "statement"
         |        }
         |      },
         |      {
         |        "term": {
         |          "state": "waiting"
         |        }
         |      },
         |      {
         |        "term": {
         |          "sessionId": "$sessionId"
         |        }
         |      },
         |      {
         |        "term": {
         |          "dataSourceName": "$dataSource"
         |        }
         |      },
         |      {
         |        "range": {
         |          "submitTime": { "gte": "now-1h" }
         |        }
         |      }
         |    ]
         |  }
         |}""".stripMargin

    val flintReader = osClient.createQueryReader(sessionIndex, dsl, "submitTime", SortOrder.ASC)
    flintReader
  }

  def addShutdownHook(
      flintSessionIndexUpdater: OpenSearchUpdater,
      osClient: OSClient,
      sessionIndex: String,
      sessionId: String,
      sessionTimerContext: Timer.Context,
      shutdownHookManager: ShutdownHookManagerTrait = DefaultShutdownHookManager): Unit = {

    shutdownHookManager.addShutdownHook(() => {
      logInfo("Shutting down REPL")
      val getResponse = osClient.getDoc(sessionIndex, sessionId)
      if (!getResponse.isExists()) {
        return
      }

      val source = getResponse.getSourceAsMap
      if (source == null) {
        return
      }

      val state = Option(source.get("state")).map(_.asInstanceOf[String])
      // It's essential to check the earlyExitFlag before marking the session state as 'dead'. When this flag is true,
      // it indicates that the control plane has already initiated a new session to handle remaining requests for the
      // current session. In our SQL setup, marking a session as 'dead' automatically triggers the creation of a new
      // session. However, the newly created session (initiated by the control plane) will enter a spin-wait state,
      // where it inefficiently waits for certain conditions to be met, leading to unproductive resource consumption
      // and eventual timeout. To avoid this issue and prevent the creation of redundant sessions by SQL, we ensure
      // the session state is not set to 'dead' when earlyExitFlag is true, thereby preventing unnecessary duplicate
      // processing.
      if (!earlyExitFlag && state.isDefined && state.get != "dead" && state.get != "fail") {
        updateFlintInstanceBeforeShutdown(
          source,
          getResponse,
          flintSessionIndexUpdater,
          sessionId,
          sessionTimerContext)
      }
    })
  }

  private def updateFlintInstanceBeforeShutdown(
      source: java.util.Map[String, AnyRef],
      getResponse: GetResponse,
      flintSessionIndexUpdater: OpenSearchUpdater,
      sessionId: String,
      sessionTimerContext: Timer.Context): Unit = {
    val flintInstance = FlintInstance.deserializeFromMap(source)
    flintInstance.state = "dead"
    flintSessionIndexUpdater.updateIf(
      sessionId,
      FlintInstance.serializeWithoutJobId(
        flintInstance,
        currentTimeProvider.currentEpochMillis()),
      getResponse.getSeqNo,
      getResponse.getPrimaryTerm)
    recordSessionSuccess(sessionTimerContext)
  }

  /**
   * Create a new thread to update the last update time of the flint instance.
   * @param currentInterval
   *   the interval of updating the last update time. Unit is millisecond.
   * @param flintSessionUpdater
   *   the updater of the flint instance.
   * @param sessionId
   *   the session id of the flint instance.
   * @param threadPool
   *   the thread pool.
   * @param osClient
   *   the OpenSearch client.
   * @param initialDelayMillis
   *   the intial delay to start heartbeat
   */
  def createHeartBeatUpdater(
      currentInterval: Long,
      flintSessionUpdater: OpenSearchUpdater,
      sessionId: String,
      threadPool: ScheduledExecutorService,
      osClient: OSClient,
      sessionIndex: String,
      initialDelayMillis: Long): ScheduledFuture[_] = {

    threadPool.scheduleAtFixedRate(
      new Runnable {
        override def run(): Unit = {
          try {
            // Check the thread's interrupt status at the beginning of the run method
            if (Thread.interrupted()) {
              logWarning("HeartBeatUpdater has been interrupted. Terminating.")
              return // Exit the run method if the thread is interrupted
            }

            flintSessionUpdater.upsert(
              sessionId,
              Serialization.write(
                Map(
                  "lastUpdateTime" -> currentTimeProvider.currentEpochMillis(),
                  "state" -> "running")))
          } catch {
            case ie: InterruptedException =>
              // Preserve the interrupt status
              Thread.currentThread().interrupt()
              logError("HeartBeatUpdater task was interrupted", ie)
              incrementCounter(
                MetricConstants.REQUEST_METADATA_HEARTBEAT_FAILED_METRIC
              ) // Record heartbeat failure metric
            // maybe due to invalid sequence number or primary term
            case e: Exception =>
              logWarning(
                s"""Fail to update the last update time of the flint instance ${sessionId}""",
                e)
              incrementCounter(
                MetricConstants.REQUEST_METADATA_HEARTBEAT_FAILED_METRIC
              ) // Record heartbeat failure metric
          }
        }
      },
      initialDelayMillis,
      currentInterval,
      java.util.concurrent.TimeUnit.MILLISECONDS)
  }

  /**
   * Reads the session store to get excluded jobs and the current job ID. If the current job ID
   * (myJobId) is not the running job ID (runJobId), or if myJobId is in the list of excluded
   * jobs, it returns false. The first condition ensures we only have one active job for one
   * session thus avoid race conditions on statement execution and states. The 2nd condition
   * ensures we don't pick up a job that has been excluded from the session store and thus CP has
   * a way to notify spark when deployments are happening. If excludeJobs is null or none of the
   * above conditions are met, it returns true.
   * @return
   *   whether we can start fetching next statement or not
   */
  def canPickNextStatement(
      sessionId: String,
      jobId: String,
      osClient: OSClient,
      sessionIndex: String): Boolean = {
    try {
      val getResponse = osClient.getDoc(sessionIndex, sessionId)
      if (getResponse.isExists()) {
        val source = getResponse.getSourceAsMap
        if (source == null) {
          logError(s"""Session id ${sessionId} is empty""")
          // still proceed since we are not sure what happened (e.g., OpenSearch cluster may be unresponsive)
          return true
        }

        val runJobId = Option(source.get("jobId")).map(_.asInstanceOf[String]).orNull
        val excludeJobIds: Seq[String] = parseExcludedJobIds(source)

        if (runJobId != null && jobId != runJobId) {
          logInfo(s"""the current job ID ${jobId} is not the running job ID ${runJobId}""")
          return false
        }
        if (excludeJobIds != null && excludeJobIds.contains(jobId)) {
          logInfo(s"""${jobId} is in the list of excluded jobs""")
          return false
        }
        true
      } else {
        // still proceed since we are not sure what happened (e.g., session doc may not be available yet)
        logError(s"""Fail to find id ${sessionId} from session index""")
        true
      }
    } catch {
      // still proceed since we are not sure what happened (e.g., OpenSearch cluster may be unresponsive)
      case e: Exception =>
        logError(s"""Fail to find id ${sessionId} from session index.""", e)
        true
    }
  }

  private def parseExcludedJobIds(source: java.util.Map[String, AnyRef]): Seq[String] = {

    val rawExcludeJobIds = source.get("excludeJobIds")
    Option(rawExcludeJobIds)
      .map {
        case s: String => Seq(s)
        case list: java.util.List[_] @unchecked =>
          import scala.collection.JavaConverters._
          list.asScala.toList
            .collect { case str: String => str } // Collect only strings from the list
        case other =>
          logInfo(s"Unexpected type: ${other.getClass.getName}")
          Seq.empty
      }
      .getOrElse(Seq.empty[String]) // In case of null, return an empty Seq
  }

  def exponentialBackoffRetry[T](maxRetries: Int, initialDelay: FiniteDuration)(
      block: => T): T = {
    var retries = 0
    var result: Option[T] = None
    var toContinue = true
    while (retries < maxRetries && toContinue) {
      try {
        result = Some(block)
        toContinue = false
      } catch {
        /*
         * If `request_index` is unavailable, the system attempts to retry up to five times. After unsuccessful retries,
         * the session state is set to 'failed', and the job will terminate. There are cases where `request_index`
         * unavailability might prevent the Spark job from updating the session state, leading to it erroneously remaining
         * as 'not_started' or 'running'. While 'not_started' is not problematic, a 'running' status requires the plugin
         * to handle it effectively to prevent inconsistencies. Notably, there's a bug where InteractiveHandler fails to
         * invalidate a REPL session with an outdated `lastUpdateTime`. This issue is documented as sql#2415 and must be
         * resolved to maintain system reliability.
         */
        case e: RuntimeException
            if e.getCause != null && e.getCause.isInstanceOf[ConnectException] =>
          retries += 1
          val delay = initialDelay * math.pow(2, retries - 1).toLong
          logError(s"Fail to connect to OpenSearch cluster. Retrying in $delay...", e)
          Thread.sleep(delay.toMillis)

        case e: Exception =>
          throw e
      }
    }

    result.getOrElse(throw new RuntimeException("Failed after retries"))
  }

  private def recordSessionSuccess(sessionTimerContext: Timer.Context): Unit = {
    stopTimer(sessionTimerContext)
    if (sessionRunningCount.get() > 0) {
      sessionRunningCount.decrementAndGet()
    }
    incrementCounter(MetricConstants.REPL_SUCCESS_METRIC)
  }

  private def recordSessionFailed(sessionTimerContext: Timer.Context): Unit = {
    stopTimer(sessionTimerContext)
    if (sessionRunningCount.get() > 0) {
      sessionRunningCount.decrementAndGet()
    }
    incrementCounter(MetricConstants.REPL_FAILED_METRIC)
  }

  private def recordStatementStateChange(
      flintCommand: FlintCommand,
      statementTimerContext: Timer.Context): Unit = {
    stopTimer(statementTimerContext)
    if (statementRunningCount.get() > 0) {
      statementRunningCount.decrementAndGet()
    }
    if (flintCommand.isComplete()) {
      incrementCounter(MetricConstants.STATEMENT_SUCCESS_METRIC)
    } else if (flintCommand.isFailed()) {
      incrementCounter(MetricConstants.STATEMENT_FAILED_METRIC)
    }
  }
}
