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
import scala.util.control.NonFatal

import com.codahale.metrics.Timer
import org.opensearch.common.Strings
import org.opensearch.flint.core.FlintOptions
import org.opensearch.flint.core.logging.CustomLogging
import org.opensearch.flint.core.metrics.MetricConstants
import org.opensearch.flint.core.metrics.MetricsUtil.{getTimerContext, incrementCounter, registerGauge, stopTimer}
import org.opensearch.flint.data.{FlintCommand, FlintInstance}

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import org.apache.spark.sql.flint.config.FlintSparkConf
import org.apache.spark.util.{ThreadUtils, Utils}

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

  private val sessionRunningCount = new AtomicInteger(0)

  def main(args: Array[String]) {
    val (queryOption, resultIndex) = parseArgs(args)

    if (Strings.isNullOrEmpty(resultIndex)) {
      logAndThrow("resultIndex is not set")
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

    val sessionId: Option[String] = Option(conf.get(FlintSparkConf.SESSION_ID.key, null))
    if (sessionId.isEmpty) {
      logAndThrow(FlintSparkConf.SESSION_ID.key + " is not set")
    }

    val jobType = conf.get(FlintSparkConf.JOB_TYPE.key, FlintSparkConf.JOB_TYPE.defaultValue.get)
    CustomLogging.logInfo(s"""Job type is: ${FlintSparkConf.JOB_TYPE.defaultValue.get}""")
    conf.set(FlintSparkConf.JOB_TYPE.key, jobType)

    val query = getQuery(queryOption, jobType, conf)

    if (jobType.equalsIgnoreCase("streaming")) {
      logInfo(s"""streaming query ${query}""")
      configDYNMaxExecutors(conf, jobType)
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

      val spark = createSparkSession(conf)
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

      // TODO: Refactor with DI
      val options = FlintSparkConf().flintOptions()
      logInfo("options.getCustomSessionManager: " + options.getCustomSessionManager())
      val sessionManager =
        instantiateProvider[SessionManager](options.getCustomSessionManager())
      logInfo("sessionManager: " + sessionManager.getClass.getSimpleName)

      /**
       * Transition the session update logic from {@link
       * org.apache.spark.util.ShutdownHookManager} to {@link SparkListenerApplicationEnd}. This
       * change helps prevent interruptions to asynchronous SigV4A signing during REPL shutdown.
       *
       * Cancelling an EMR job directly when SigV4a signer in use could otherwise lead to stale
       * sessions. For tracking, see the GitHub issue:
       * https://github.com/opensearch-project/opensearch-spark/issues/320
       */
      spark.sparkContext.addSparkListener(new PreShutdownListener(sessionManager, sessionId.get))

      // 1 thread for updating heart beat
      val threadPool =
        threadPoolFactory.newDaemonThreadPoolScheduledExecutor("flint-repl-heartbeat", 1)

      registerGauge(MetricConstants.REPL_RUNNING_METRIC, sessionRunningCount)
      val sessionTimerContext = getTimerContext(MetricConstants.REPL_PROCESSING_TIME_METRIC)

      val jobStartTime = currentTimeProvider.currentEpochMillis()
      // update heart beat every 30 seconds
      // OpenSearch triggers recovery after 1 minute outdated heart beat
      var heartBeatFuture: ScheduledFuture[_] = null
      try {
        heartBeatFuture = createHeartBeatUpdater(
          HEARTBEAT_INTERVAL_MILLIS,
          sessionManager,
          sessionId.get,
          threadPool,
          INITIAL_DELAY_MILLIS)

        val confExcludeJobsOpt = conf.getOption(FlintSparkConf.EXCLUDE_JOB_IDS.key)
        confExcludeJobsOpt match {
          case None =>
            // If confExcludeJobs is None, pass null or an empty sequence as per your setupFlintJob method's signature
            setupFlintJob(applicationId, jobId, sessionId.get, sessionManager, jobStartTime)

          case Some(confExcludeJobs) =>
            // example: --conf spark.flint.deployment.excludeJobs=job-1,job-2
            val excludeJobIds = confExcludeJobs.split(",").toList // Convert Array to Lis

            if (excludeJobIds.contains(jobId)) {
              logInfo(s"current job is excluded, exit the application.")
              earlyExitFlag = true
              return
            }

            if (sessionManager.exclusionCheck(sessionId.get, excludeJobIds)) {
              earlyExitFlag = true
              return
            }

            setupFlintJob(
              applicationId,
              jobId,
              sessionId.get,
              sessionManager,
              jobStartTime,
              excludeJobIds)
        }

        val name = spark.conf.get(FlintSparkConf.CUSTOM_QUERY_RESULT_WRITER.key)
        logInfo("queryResultWriter name: " + name)
        val queryResultWriter =
          instantiateWriter[QueryResultWriter](name)

        val commandContext = CommandContext(
          spark,
          dataSource,
          resultIndex,
          sessionId.get,
          sessionManager,
          queryResultWriter,
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
            sessionManager,
            jobStartTime,
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

  def getQuery(queryOption: Option[String], jobType: String, conf: SparkConf): String = {
    queryOption.getOrElse {
      if (jobType.equalsIgnoreCase("streaming")) {
        val defaultQuery = conf.get(FlintSparkConf.QUERY.key, "")
        if (defaultQuery.isEmpty) {
          logAndThrow("Query undefined for the streaming job.")
        }
        unescapeQuery(defaultQuery)
      } else ""
    }
  }

  def queryLoop(commandContext: CommandContext): Unit = {
    // 1 thread for updating heart beat
    val threadPool = threadPoolFactory.newDaemonThreadPoolScheduledExecutor("flint-repl-query", 1)
    implicit val executionContext = ExecutionContext.fromExecutor(threadPool)

    // TODO: Extend this to support other class
    val options = FlintSparkConf().flintOptions()
    val commandLifecycleManager = instantiateProvider[CommandLifecycleManager](
      options.getCustomCommandLifecycleManager,
      commandContext)

    var futurePrepareCommandExecution: Future[Either[String, Unit]] = null
    try {
      futurePrepareCommandExecution = Future {
        commandLifecycleManager.prepareCommandLifecycle()
      }

      var lastActivityTime = currentTimeProvider.currentEpochMillis()
      var verificationResult: VerificationResult = NotVerified
      var canPickUpNextStatement = true
      var lastCanPickCheckTime = 0L
      while (currentTimeProvider
          .currentEpochMillis() - lastActivityTime <= commandContext.inactivityLimitMillis && canPickUpNextStatement) {
        logInfo(s"""read from sessionId: ${commandContext.sessionId}""")

        try {
          // TODO: Refactor CommandContext and CommandState
          val commandState = CommandState(
            lastActivityTime,
            verificationResult,
            commandLifecycleManager,
            futurePrepareCommandExecution,
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
          commandLifecycleManager.closeCommandLifecycle()
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
      sessionManager: SessionManager,
      jobStartTime: Long,
      excludeJobIds: Seq[String] = Seq.empty[String]): Unit = {
    val flintJob = new FlintInstance(
      applicationId,
      jobId,
      sessionId,
      "running",
      currentTimeProvider.currentEpochMillis(),
      jobStartTime,
      excludeJobIds)
    sessionManager.updateSessionDetails(flintJob, updateMode = UpdateMode.Upsert)
    sessionRunningCount.incrementAndGet()
  }

  private def handleSessionError(
      e: Exception,
      applicationId: String,
      jobId: String,
      sessionId: String,
      sessionManager: SessionManager,
      jobStartTime: Long,
      sessionTimerContext: Timer.Context): Unit = {
    val error = s"Session error: ${e.getMessage}"
    CustomLogging.logError(error, e)

    // TODO: Refactor this
    val flintInstance = sessionManager
      .getSessionDetails(sessionId)
      .getOrElse(
        new FlintInstance(
          applicationId,
          jobId,
          sessionId,
          "fail",
          currentTimeProvider.currentEpochMillis(),
          jobStartTime,
          error = Some(error)))

    sessionManager.updateSessionDetails(flintInstance, UpdateMode.Upsert)
    if (flintInstance.state.equals("fail")) {
      recordSessionFailed(sessionTimerContext)
    }
  }

  // TODO: Refactor this with new writer interface
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

  def processQueryException(ex: Exception, flintCommand: FlintCommand): String = {
    val error = super.processQueryException(ex)
    flintCommand.fail()
    flintCommand.error = Some(error)
    error
  }

  // TODO: Refactor
  private def processCommands(
      context: CommandContext,
      state: CommandState): (Long, VerificationResult, Boolean, Long) = {
    import context._
    import state._

    var lastActivityTime = recordedLastActivityTime
    var verificationResult = recordedVerificationResult
    var canProceed = true
    var canPickNextStatementResult = true
    var lastCanPickCheckTime = recordedLastCanPickCheckTime

    while (canProceed) {
      val currentTime = currentTimeProvider.currentEpochMillis()

      // Only call canPickNextStatement if EARLY_TERMIANTION_CHECK_FREQUENCY milliseconds have passed
      if (currentTime - lastCanPickCheckTime > EARLY_TERMIANTION_CHECK_FREQUENCY) {
        canPickNextStatementResult = sessionManager.canPickNextCommand(sessionId, jobId)
        lastCanPickCheckTime = currentTime
      }

      if (!canPickNextStatementResult) {
        earlyExitFlag = true
        canProceed = false
      } else if (!commandLifecycleManager.hasPendingCommand(sessionId)) {
        canProceed = false
      } else {
        val flintCommand = commandLifecycleManager.initCommandLifecycle(sessionId)
        val (dataToWrite, returnedVerificationResult) = processStatementOnVerification(
          recordedVerificationResult,
          flintCommand,
          context,
          executionContext,
          futurePrepareCommandExecution)

        verificationResult = returnedVerificationResult
        try {
          dataToWrite.foreach(df => logInfo("DF: " + df))

          if (flintCommand.isRunning() || flintCommand.isWaiting()) {
            // we have set failed state in exception handling
            flintCommand.complete()
          }
          commandLifecycleManager.updateCommandDetails(flintCommand)
        } catch {
          // e.g., maybe due to authentication service connection issue
          // or invalid catalog (e.g., we are operating on data not defined in provided data source)
          case e: Exception =>
            val error = s"""Fail to write result of ${flintCommand}, cause: ${e.getMessage}"""
            CustomLogging.logError(error, e)
            flintCommand.fail()
            commandLifecycleManager.updateCommandDetails(flintCommand)
        }
        // last query finish time is last activity time
        lastActivityTime = currentTimeProvider.currentEpochMillis()
      }
    }

    // return tuple indicating if still active and mapping verification result
    (lastActivityTime, verificationResult, canPickNextStatementResult, lastCanPickCheckTime)
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
    Some(
      handleCommandFailureAndGetFailedData(
        spark,
        dataSource,
        error,
        flintCommand,
        sessionId,
        startTime))
  }

  // TODO: Refactor
  def executeAndHandle(
      spark: SparkSession,
      flintCommand: FlintCommand,
      dataSource: String,
      sessionId: String,
      executionContext: ExecutionContextExecutor,
      startTime: Long,
      queryExecuitonTimeOut: Duration,
      queryWaitTimeMillis: Long,
      queryResultWriter: QueryResultWriter): Option[DataFrame] = {
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
          queryWaitTimeMillis,
          queryResultWriter))
    } catch {
      case e: TimeoutException =>
        val error = s"Executing ${flintCommand.query} timed out"
        CustomLogging.logError(error, e)
        handleCommandTimeout(spark, dataSource, error, flintCommand, sessionId, startTime)
      case e: Exception =>
        logInfo("commitID: 017f1635b004e2f6277fb84ede444f6cc211bcbe")
        logInfo("Louis E: " + e.getClass.getSimpleName)
        throw e
//        val error = processQueryException(e, flintCommand)
//        Some(
//          handleCommandFailureAndGetFailedData(
//            spark,
//            dataSource,
//            error,
//            flintCommand,
//            sessionId,
//            startTime))
    }
  }

  private def processStatementOnVerification(
      recordedVerificationResult: VerificationResult,
      flintCommand: FlintCommand,
      context: CommandContext,
      executionContext: ExecutionContextExecutor,
      futurePrepareCommandExecution: Future[Either[String, Unit]])
      : (Option[DataFrame], VerificationResult) = {
    import context._

    val startTime: Long = currentTimeProvider.currentEpochMillis()
    var verificationResult = recordedVerificationResult
    var dataToWrite: Option[DataFrame] = None

    verificationResult match {
      case NotVerified =>
        try {
          ThreadUtils.awaitResult(futurePrepareCommandExecution, MAPPING_CHECK_TIMEOUT) match {
            case Right(_) =>
              dataToWrite = executeAndHandle(
                spark,
                flintCommand,
                dataSource,
                sessionId,
                executionContext,
                startTime,
                queryExecutionTimeout,
                queryWaitTimeMillis,
                queryResultWriter)
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
            CustomLogging.logError(error, e)
            dataToWrite =
              handleCommandTimeout(spark, dataSource, error, flintCommand, sessionId, startTime)
          case NonFatal(e) =>
            val error = s"An unexpected error occurred: ${e.getMessage}"
            CustomLogging.logError(error, e)
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
          queryWaitTimeMillis,
          queryResultWriter)
    }

    logInfo(s"command complete: $flintCommand")
    (dataToWrite, verificationResult)
  }

  // TODO: Refactor
  def executeQueryAsync(
      spark: SparkSession,
      flintCommand: FlintCommand,
      dataSource: String,
      sessionId: String,
      executionContext: ExecutionContextExecutor,
      startTime: Long,
      queryExecutionTimeOut: Duration,
      queryWaitTimeMillis: Long,
      queryResultWriter: QueryResultWriter): DataFrame = {
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
      logInfo("1-commitID - b933e5df61002d72d496cac4e0ea9099a4cfe017")
      val futureQueryExecution = Future {
        executeQuery(
          spark,
          flintCommand,
          dataSource,
          flintCommand.queryId,
          sessionId,
          false,
          queryResultWriter)
      }(executionContext)

      // time out after 10 minutes
      ThreadUtils.awaitResult(futureQueryExecution, queryExecutionTimeOut)
    }
  }

  class PreShutdownListener(sessionManager: SessionManager, sessionId: String)
      extends SparkListener
      with Logging {

    override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
      logInfo("Shutting down REPL")
      logInfo("earlyExitFlag: " + earlyExitFlag)
      try {
        val sessionDetails = sessionManager.getSessionDetails(sessionId).get
        if (!earlyExitFlag && sessionDetails.state != "dead" && sessionDetails.state != "fail") {
          sessionDetails.state = "dead"
          sessionManager.updateSessionDetails(sessionDetails, updateMode = UpdateMode.UpdateIf)
        }
      } catch {
        case e: Exception => logError(s"Failed to update session state for $sessionId", e)
      }
    }
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
      sessionManager: SessionManager,
      sessionId: String,
      threadPool: ScheduledExecutorService,
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

            sessionManager.recordHeartbeat(sessionId)
          } catch {
            case ie: InterruptedException =>
              // Preserve the interrupt status
              Thread.currentThread().interrupt()
              CustomLogging.logError("HeartBeatUpdater task was interrupted", ie)
              incrementCounter(
                MetricConstants.REQUEST_METADATA_HEARTBEAT_FAILED_METRIC
              ) // Record heartbeat failure metric
            // maybe due to invalid sequence number or primary term
            case e: Exception =>
              CustomLogging.logWarning(
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
          CustomLogging.logError(
            s"Fail to connect to OpenSearch cluster. Retrying in $delay...",
            e)
          Thread.sleep(delay.toMillis)

        case e: Exception =>
          CustomLogging.logError(e)
          throw e
      }
    }

    result.getOrElse(throw new RuntimeException("Failed after retries"))
  }

  private def recordSessionSuccess(sessionTimerContext: Timer.Context): Unit = {
    logInfo("Session Success")
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

  private def instantiateProvider[T](className: String): T = {
    try {
      val providerClass = Utils.classForName(className)
      val ctor = providerClass.getDeclaredConstructor(classOf[FlintSparkConf])
      ctor.setAccessible(true)
      ctor.newInstance(FlintSparkConf()).asInstanceOf[T]
    } catch {
      case e: Exception =>
        throw new RuntimeException(s"Failed to instantiate provider: $className", e)
    }
  }

  private def instantiateProvider[T](className: String, context: CommandContext): T = {
    try {
      val providerClass = Utils.classForName(className)
      val ctor = providerClass.getDeclaredConstructor(classOf[CommandContext])
      ctor.setAccessible(true)
      ctor.newInstance(context).asInstanceOf[T]
    } catch {
      case e: Exception =>
        throw new RuntimeException(s"Failed to instantiate provider: $className", e)
    }
  }

  private def instantiateWriter[T](className: String): T = {
    try {
      val providerClass = Utils.classForName(className)
      val ctor = providerClass.getDeclaredConstructor()
      ctor.setAccessible(true)
      ctor.newInstance().asInstanceOf[T]
    } catch {
      case e: Exception =>
        throw new RuntimeException(s"Failed to instantiate provider: $className", e)
    }
  }
}
