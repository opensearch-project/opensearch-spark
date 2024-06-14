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
import org.opensearch.flint.core.FlintOptions
import org.opensearch.flint.core.logging.CustomLogging
import org.opensearch.flint.core.metrics.MetricConstants
import org.opensearch.flint.core.metrics.MetricsUtil.{getTimerContext, incrementCounter, registerGauge, stopTimer}
import org.opensearch.flint.data.{FlintStatement, InteractiveSession, SessionStates}

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import org.apache.spark.sql.SessionUpdateMode.UPDATE_IF
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
  private val PREPARE_QUERY_EXEC_TIMEOUT = Duration(1, MINUTES)
  private val DEFAULT_QUERY_EXECUTION_TIMEOUT = Duration(30, MINUTES)
  private val DEFAULT_QUERY_WAIT_TIMEOUT_MILLIS = 10 * 60 * 1000
  private val INITIAL_DELAY_MILLIS = 3000L
  private val EARLY_TERMINATION_CHECK_FREQUENCY = 60000L

  @volatile var earlyExitFlag: Boolean = false

  private val sessionRunningCount = new AtomicInteger(0)
  private val statementRunningCount = new AtomicInteger(0)

  def main(args: Array[String]) {
    val (queryOption, resultIndex) = parseArgs(args)

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
    CustomLogging.logInfo(s"""Job type is: ${FlintSparkConf.JOB_TYPE.defaultValue.get}""")
    conf.set(FlintSparkConf.JOB_TYPE.key, jobType)

    val query = getQuery(queryOption, jobType, conf)

    if (jobType.equalsIgnoreCase("streaming")) {
      logInfo(s"streaming query $query")
      resultIndex match {
        case Some(index) =>
          configDYNMaxExecutors(conf, jobType)
          val streamingRunningCount = new AtomicInteger(0)
          val jobOperator = JobOperator(
            createSparkSession(conf),
            query,
            dataSource,
            index, // Ensure the correct Option type is passed
            true,
            streamingRunningCount)
          registerGauge(MetricConstants.STREAMING_RUNNING_METRIC, streamingRunningCount)
          jobOperator.start()

        case None => logAndThrow("resultIndex is not set")
      }
    } else {
      // we don't allow default value for sessionIndex and sessionId. Throw exception if key not found.
      val sessionId: Option[String] = Option(conf.get(FlintSparkConf.SESSION_ID.key, null))

      if (sessionId.isEmpty) {
        logAndThrow(FlintSparkConf.SESSION_ID.key + " is not set")
      }

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

      val sessionManager = instantiateSessionManager(spark, resultIndex)
      val sessionTimerContext = getTimerContext(MetricConstants.REPL_PROCESSING_TIME_METRIC)

      /**
       * Transition the session update logic from {@link
       * org.apache.spark.util.ShutdownHookManager} to {@link SparkListenerApplicationEnd}. This
       * change helps prevent interruptions to asynchronous SigV4A signing during REPL shutdown.
       *
       * Cancelling an EMR job directly when SigV4a signer in use could otherwise lead to stale
       * sessions. For tracking, see the GitHub issue:
       * https://github.com/opensearch-project/opensearch-spark/issues/320
       */
      spark.sparkContext.addSparkListener(
        new PreShutdownListener(sessionId.get, sessionManager, sessionTimerContext))

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
          sessionId.get,
          sessionManager,
          HEARTBEAT_INTERVAL_MILLIS,
          INITIAL_DELAY_MILLIS,
          threadPool)

        if (setupFlintJobWithExclusionCheck(
            conf,
            applicationId,
            jobId,
            sessionId.get,
            sessionManager,
            jobStartTime)) {
          earlyExitFlag = true
          return
        }

        val queryExecutionManager =
          instantiateQueryExecutionManager(spark, sessionManager.getSessionManagerMetadata)
        val queryResultWriter =
          instantiateQueryResultWriter(spark, sessionManager.getSessionManagerMetadata)
        val queryLoopContext = StatementExecutionContext(
          spark,
          jobId,
          sessionId.get,
          sessionManager,
          queryExecutionManager,
          queryResultWriter,
          dataSource,
          queryExecutionTimeoutSecs,
          inactivityLimitMillis,
          queryWaitTimeoutMillis)
        exponentialBackoffRetry(maxRetries = 5, initialDelay = 2.seconds) {
          queryLoop(queryLoopContext)
        }
        recordSessionSuccess(sessionTimerContext)
      } catch {
        case e: Exception =>
          handleSessionError(
            applicationId,
            jobId,
            sessionId.get,
            sessionManager,
            sessionTimerContext,
            jobStartTime,
            e)
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
      applicationId: String,
      jobId: String,
      sessionId: String,
      sessionManager: SessionManager,
      jobStartTime: Long): Boolean = {
    val confExcludeJobsOpt = conf.getOption(FlintSparkConf.EXCLUDE_JOB_IDS.key)

    confExcludeJobsOpt match {
      case None =>
        // If confExcludeJobs is None, pass null or an empty sequence as per your setupFlintJob method's signature
        setupFlintJob(applicationId, jobId, sessionId, sessionManager, jobStartTime)

      case Some(confExcludeJobs) =>
        // example: --conf spark.flint.deployment.excludeJobs=job-1,job-2
        val excludedJobIds = confExcludeJobs.split(",").toList // Convert Array to Lis

        if (excludedJobIds.contains(jobId)) {
          logInfo(s"current job is excluded, exit the application.")
          return true
        }

        val sessionDetails = sessionManager.getSessionDetails(sessionId)
        val existingExcludedJobIds = sessionDetails.get.excludedJobIds
        if (excludedJobIds.sorted == existingExcludedJobIds.sorted) {
          logInfo("duplicate job running, exit the application.")
          return true
        }

        // If none of the edge cases are met, proceed with setup
        setupFlintJob(
          applicationId,
          jobId,
          sessionId,
          sessionManager,
          jobStartTime,
          excludedJobIds = excludedJobIds)
    }
    false
  }

  def queryLoop(context: StatementExecutionContext): Unit = {
    import context._
    // 1 thread for query execution preparation
    val threadPool = threadPoolFactory.newDaemonThreadPoolScheduledExecutor("flint-repl-query", 1)
    implicit val futureExecutor = ExecutionContext.fromExecutor(threadPool)
    var futurePrepareQueryExecution: Future[Either[String, Unit]] = null
    try {
      futurePrepareQueryExecution = Future {
        statementLifecycleManager.prepareStatementLifecycle()
      }

      var lastActivityTime = currentTimeProvider.currentEpochMillis()
      var verificationResult: VerificationResult = NotVerified
      var canPickUpNextStatement = true
      var lastCanPickCheckTime = 0L
      while (currentTimeProvider
          .currentEpochMillis() - lastActivityTime <= context.inactivityLimitMillis && canPickUpNextStatement) {
        logInfo(s"""sessionId: ${context.sessionId}""")

        try {
          val inMemoryQueryExecutionState = InMemoryQueryExecutionState(
            lastActivityTime,
            lastCanPickCheckTime,
            verificationResult,
            futurePrepareQueryExecution,
            futureExecutor)
          val result: (Long, VerificationResult, Boolean, Long) =
            processStatements(context, inMemoryQueryExecutionState)

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
          statementLifecycleManager.terminateStatementLifecycle()
        }

        Thread.sleep(100)
      }
    } finally {
      if (threadPool != null) {
        threadPoolFactory.shutdownThreadPool(threadPool)
      }
    }
  }

  private def refreshSessionState(
      applicationId: String,
      jobId: String,
      sessionId: String,
      sessionManager: SessionManager,
      jobStartTime: Long,
      state: String,
      error: Option[String] = None,
      excludedJobIds: Seq[String] = Seq.empty[String]): InteractiveSession = {

    val sessionDetails = sessionManager
      .getSessionDetails(sessionId)
      .getOrElse(
        new InteractiveSession(
          applicationId,
          jobId,
          sessionId,
          state,
          currentTimeProvider.currentEpochMillis(),
          jobStartTime,
          error = error,
          excludedJobIds = excludedJobIds))
    sessionDetails.state = state
    sessionManager.updateSessionDetails(sessionDetails, updateMode = SessionUpdateMode.UPSERT)
    sessionDetails
  }

  private def setupFlintJob(
      applicationId: String,
      jobId: String,
      sessionId: String,
      sessionManager: SessionManager,
      jobStartTime: Long,
      excludedJobIds: Seq[String] = Seq.empty[String]): Unit = {
    refreshSessionState(
      applicationId,
      jobId,
      sessionId,
      sessionManager,
      jobStartTime,
      SessionStates.RUNNING,
      excludedJobIds = excludedJobIds)
    sessionRunningCount.incrementAndGet()
  }

  private def handleSessionError(
      applicationId: String,
      jobId: String,
      sessionId: String,
      sessionManager: SessionManager,
      sessionTimerContext: Timer.Context,
      jobStartTime: Long,
      e: Exception): Unit = {
    val error = s"Session error: ${e.getMessage}"
    CustomLogging.logError(error, e)
    refreshSessionState(
      applicationId,
      jobId,
      sessionId,
      sessionManager,
      jobStartTime,
      SessionStates.FAIL,
      Some(e.getMessage))
    recordSessionFailed(sessionTimerContext)
  }

  /**
   * handling the case where a command's execution fails, updates the flintStatement with the
   * error and failure status, and then write the result to result index. Thus, an error is
   * written to both result index or statement model in request index
   *
   * @param spark
   *   spark session
   * @param dataSource
   *   data source
   * @param error
   *   error message
   * @param flintStatement
   *   flint command
   * @param sessionId
   *   session id
   * @param startTime
   *   start time
   * @return
   *   failed data frame
   */
  def handleStatementFailureAndGetFailedData(
      spark: SparkSession,
      dataSource: String,
      error: String,
      flintStatement: FlintStatement,
      sessionId: String): DataFrame = {
    flintStatement.fail()
    flintStatement.error = Some(error)
    super.getFailedData(
      spark,
      dataSource,
      error,
      flintStatement.queryId,
      flintStatement.query,
      sessionId,
      flintStatement.queryStartTime.get)
  }

  def processQueryException(ex: Exception, flintStatement: FlintStatement): String = {
    val error = super.processQueryException(ex)
    flintStatement.fail()
    flintStatement.error = Some(error)
    error
  }

  private def processStatements(
      context: StatementExecutionContext,
      state: InMemoryQueryExecutionState): (Long, VerificationResult, Boolean, Long) = {
    import context._
    import state._

    var lastActivityTime = recordedLastActivityTime
    var verificationResult = recordedVerificationResult
    var canProceed = true
    var canPickNextStatementResult = true // Add this line to keep track of canPickNextStatement
    var lastCanPickCheckTime = recordedLastCanPickCheckTime

    while (canProceed) {
      val currentTime = currentTimeProvider.currentEpochMillis()

      // Only call canPickNextStatement if EARLY_TERMINATION_CHECK_FREQUENCY milliseconds have passed
      if (currentTime - lastCanPickCheckTime > EARLY_TERMINATION_CHECK_FREQUENCY) {
        canPickNextStatementResult = canPickNextStatement(sessionManager, sessionId, jobId)
        lastCanPickCheckTime = currentTime
      }

      if (!canPickNextStatementResult) {
        earlyExitFlag = true
        canProceed = false
      } else {
        sessionManager.getNextStatement(sessionId) match {
          case Some(flintStatement) =>
            val statementTimerContext = getTimerContext(
              MetricConstants.STATEMENT_PROCESSING_TIME_METRIC)

            val (dataToWrite, returnedVerificationResult) =
              processStatementOnVerification(flintStatement, state, context)

            verificationResult = returnedVerificationResult
            finalizeStatement(context, dataToWrite, flintStatement, statementTimerContext)
            // last query finish time is last activity time
            lastActivityTime = currentTimeProvider.currentEpochMillis()

          case None =>
            canProceed = false
        }
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
   * @param flintStatement
   *   flint command
   * @param resultIndex
   *   result index
   * @param flintSessionIndexUpdater
   *   flint session index updater
   */
  private def finalizeStatement(
      context: StatementExecutionContext,
      dataToWrite: Option[DataFrame],
      flintStatement: FlintStatement,
      statementTimerContext: Timer.Context): Unit = {
    import context._

    try {
      dataToWrite.foreach(df => queryResultWriter.persistQueryResult(df, flintStatement))
      if (flintStatement.isRunning || flintStatement.isWaiting) {
        // we have set failed state in exception handling
        flintStatement.complete()
      }
    } catch {
      // e.g., maybe due to authentication service connection issue
      // or invalid catalog (e.g., we are operating on data not defined in provided data source)
      case e: Exception =>
        val error = s"""Fail to write result of ${flintStatement}, cause: ${e.getMessage}"""
        CustomLogging.logError(error, e)
        flintStatement.fail()
    } finally {
      statementLifecycleManager.updateStatement(flintStatement)
      recordStatementStateChange(flintStatement, statementTimerContext)
    }
  }

  private def handleStatementTimeout(
      spark: SparkSession,
      dataSource: String,
      error: String,
      flintStatement: FlintStatement,
      sessionId: String) = {
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
    spark.sparkContext.cancelJobGroup(flintStatement.queryId)
    Some(
      handleStatementFailureAndGetFailedData(spark, dataSource, error, flintStatement, sessionId))
  }

  def executeAndHandle(
      flintStatement: FlintStatement,
      state: InMemoryQueryExecutionState,
      context: StatementExecutionContext): Option[DataFrame] = {
    import context._

    try {
      Some(executeQueryAsync(flintStatement, state, context))
    } catch {
      case e: TimeoutException =>
        val error = s"Executing ${flintStatement.query} timed out"
        CustomLogging.logError(error, e)
        handleStatementTimeout(spark, dataSource, error, flintStatement, sessionId)
      case e: Exception =>
        val error = processQueryException(e, flintStatement)
        Some(
          handleStatementFailureAndGetFailedData(
            spark,
            dataSource,
            error,
            flintStatement,
            sessionId))
    }
  }

  private def processStatementOnVerification(
      flintStatement: FlintStatement,
      state: InMemoryQueryExecutionState,
      context: StatementExecutionContext): (Option[DataFrame], VerificationResult) = {
    import context._
    import state._

    flintStatement.queryStartTime = Some(currentTimeProvider.currentEpochMillis())
    var verificationResult = recordedVerificationResult
    var dataToWrite: Option[DataFrame] = None

    verificationResult match {
      case NotVerified =>
        try {
          ThreadUtils.awaitResult(futurePrepareQueryExecution, PREPARE_QUERY_EXEC_TIMEOUT) match {
            case Right(_) =>
              dataToWrite = executeAndHandle(flintStatement, state, context)
              verificationResult = VerifiedWithoutError
            case Left(error) =>
              verificationResult = VerifiedWithError(error)
              dataToWrite = Some(
                handleStatementFailureAndGetFailedData(
                  spark,
                  dataSource,
                  error,
                  flintStatement,
                  sessionId))
          }
        } catch {
          case e: TimeoutException =>
            val error = s"Query execution preparation timed out"
            CustomLogging.logError(error, e)
            dataToWrite =
              handleStatementTimeout(spark, dataSource, error, flintStatement, sessionId)
          case NonFatal(e) =>
            val error = s"An unexpected error occurred: ${e.getMessage}"
            CustomLogging.logError(error, e)
            dataToWrite = Some(
              handleStatementFailureAndGetFailedData(
                spark,
                dataSource,
                error,
                flintStatement,
                sessionId))
        }
      case VerifiedWithError(err) =>
        dataToWrite = Some(
          handleStatementFailureAndGetFailedData(
            spark,
            dataSource,
            err,
            flintStatement,
            sessionId))
      case VerifiedWithoutError =>
        dataToWrite = executeAndHandle(flintStatement, state, context)
    }

    logInfo(s"command complete: $flintStatement")
    (dataToWrite, verificationResult)
  }

  def executeQueryAsync(
      flintStatement: FlintStatement,
      state: InMemoryQueryExecutionState,
      context: StatementExecutionContext): DataFrame = {
    import context._
    import state._

    if (currentTimeProvider
        .currentEpochMillis() - flintStatement.submitTime > queryWaitTimeMillis) {
      handleStatementFailureAndGetFailedData(
        spark,
        dataSource,
        "wait timeout",
        flintStatement,
        sessionId)
    } else {
      val futureQueryExecution = Future {
        executeQuery(flintStatement, context)
      }(futureExecutor)

      // time out after 10 minutes
      ThreadUtils.awaitResult(futureQueryExecution, queryExecutionTimeout)
    }
  }

  def executeQuery(
      flintStatement: FlintStatement,
      context: StatementExecutionContext): DataFrame = {
    import context._
    // reset start time
    flintStatement.queryStartTime = Some(System.currentTimeMillis())
    // we have to set job group in the same thread that started the query according to spark doc
    spark.sparkContext.setJobGroup(
      flintStatement.queryId,
      "Job group for " + flintStatement.queryId,
      interruptOnCancel = true)
    // Execute SQL query
    val result: DataFrame = spark.sql(flintStatement.query)
    queryResultWriter.reformatQueryResult(result, flintStatement, context)
  }

  class PreShutdownListener(
      sessionId: String,
      sessionManager: SessionManager,
      sessionTimerContext: Timer.Context)
      extends SparkListener
      with Logging {

    override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
      logInfo("Shutting down REPL")
      logInfo("earlyExitFlag: " + earlyExitFlag)

      try {
        sessionManager.getSessionDetails(sessionId).foreach { sessionDetails =>
          // It's essential to check the earlyExitFlag before marking the session state as 'dead'. When this flag is true,
          // it indicates that the control plane has already initiated a new session to handle remaining requests for the
          // current session. In our SQL setup, marking a session as 'dead' automatically triggers the creation of a new
          // session. However, the newly created session (initiated by the control plane) will enter a spin-wait state,
          // where it inefficiently waits for certain conditions to be met, leading to unproductive resource consumption
          // and eventual timeout. To avoid this issue and prevent the creation of redundant sessions by SQL, we ensure
          // the session state is not set to 'dead' when earlyExitFlag is true, thereby preventing unnecessary duplicate
          // processing.
          if (!earlyExitFlag && !sessionDetails.isDead && !sessionDetails.isFail) {
            sessionDetails.state = SessionStates.DEAD
            sessionManager.updateSessionDetails(sessionDetails, UPDATE_IF)
            recordSessionSuccess(sessionTimerContext)
          }
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
      sessionId: String,
      sessionManager: SessionManager,
      currentInterval: Long,
      initialDelayMillis: Long,
      threadPool: ScheduledExecutorService): ScheduledFuture[_] = {

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
      sessionManager: SessionManager,
      sessionId: String,
      jobId: String): Boolean = {
    try {
      sessionManager.getSessionDetails(sessionId) match {
        case Some(sessionDetails) =>
          val runJobId = sessionDetails.jobId
          val excludeJobIds = sessionDetails.excludedJobIds

          if (!runJobId.isEmpty && jobId != runJobId) {
            logInfo(s"the current job ID $jobId is not the running job ID ${runJobId}")
            return false
          }
          if (excludeJobIds.contains(jobId)) {
            logInfo(s"$jobId is in the list of excluded jobs")
            return false
          }
          true
        case None =>
          logError(s"Failed to fetch sessionDetails by sessionId: $sessionId.")
          true
      }
    } catch {
      // still proceed with exception
      case e: Exception =>
        CustomLogging.logError(s"""Fail to find id ${sessionId} from session index.""", e)
        true
    }
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

  private def instantiateProvider[T](defaultProvider: => T, providerClassName: String): T = {
    if (providerClassName.isEmpty) {
      defaultProvider
    } else {
      try {
        val providerClass = Utils.classForName(providerClassName)
        val ctor = providerClass.getDeclaredConstructor()
        ctor.setAccessible(true)
        ctor.newInstance().asInstanceOf[T]
      } catch {
        case e: Exception =>
          throw new RuntimeException(s"Failed to instantiate provider: $providerClassName", e)
      }
    }
  }

  private def instantiateSessionManager(
      spark: SparkSession,
      resultIndex: Option[String]): SessionManager = {
    instantiateProvider(
      new SessionManagerImpl(spark, resultIndex),
      spark.conf.get(FlintSparkConf.CUSTOM_SESSION_MANAGER.key))
  }

  private def instantiateQueryExecutionManager(
      spark: SparkSession,
      context: Map[String, Any]): StatementLifecycleManager = {
    instantiateProvider(
      new StatementLifecycleManagerImpl(context),
      spark.conf.get(FlintSparkConf.CUSTOM_STATEMENT_MANAGER.key))
  }

  private def instantiateQueryResultWriter(
      spark: SparkSession,
      context: Map[String, Any]): QueryResultWriter = {
    instantiateProvider(
      new QueryResultWriterImpl(context),
      spark.conf.get(FlintSparkConf.CUSTOM_QUERY_RESULT_WRITER.key))
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

  private def recordStatementStateChange(
      flintStatement: FlintStatement,
      statementTimerContext: Timer.Context): Unit = {
    stopTimer(statementTimerContext)
    if (statementRunningCount.get() > 0) {
      statementRunningCount.decrementAndGet()
    }
    if (flintStatement.isComplete) {
      incrementCounter(MetricConstants.STATEMENT_SUCCESS_METRIC)
    } else if (flintStatement.isFailed) {
      incrementCounter(MetricConstants.STATEMENT_FAILED_METRIC)
    }
  }
}
