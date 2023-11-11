/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql

import java.net.ConnectException
import java.time.Instant
import java.util.Map
import java.util.concurrent.ScheduledExecutorService

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future, TimeoutException}
import scala.concurrent.duration._
import scala.concurrent.duration.{Duration, MINUTES}
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

import org.opensearch.action.get.GetResponse
import org.opensearch.common.Strings
import org.opensearch.flint.app.{FlintCommand, FlintInstance}
import org.opensearch.flint.core.storage.{FlintReader, OpenSearchUpdater}

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.flint.config.FlintSparkConf
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
  private val DEFAULT_INACTIVITY_LIMIT_MILLIS = 30 * 60 * 1000
  private val MAPPING_CHECK_TIMEOUT = Duration(1, MINUTES)
  private val DEFAULT_QUERY_EXECUTION_TIMEOUT = Duration(10, MINUTES)
  private val DEFAULT_QUERY_WAIT_TIMEOUT_MILLIS = 10 * 60 * 1000

  def update(flintCommand: FlintCommand, updater: OpenSearchUpdater): Unit = {
    updater.update(flintCommand.statementId, FlintCommand.serialize(flintCommand))
  }

  def main(args: Array[String]) {
    val Array(query, resultIndex) = args
    if (Strings.isNullOrEmpty(resultIndex)) {
      throw new IllegalArgumentException("resultIndex is not set")
    }

    // init SparkContext
    val conf: SparkConf = createSparkConf()
    val dataSource = conf.get("spark.flint.datasource.name", "unknown")
    // https://github.com/opensearch-project/opensearch-spark/issues/138
    conf.set("spark.sql.defaultCatalog", dataSource)
    val wait = conf.get("spark.flint.job.type", "continue")
    // we don't allow default value for sessionIndex and sessionId. Throw exception if key not found.
    val sessionIndex: Option[String] = Option(conf.get("spark.flint.job.requestIndex", null))
    val sessionId: Option[String] = Option(conf.get("spark.flint.job.sessionId", null))

    if (sessionIndex.isEmpty) {
      throw new IllegalArgumentException("spark.flint.job.requestIndex is not set")
    }
    if (sessionId.isEmpty) {
      throw new IllegalArgumentException("spark.flint.job.sessionId is not set")
    }

    val spark = createSparkSession(conf)
    val osClient = new OSClient(FlintSparkConf().flintOptions())
    val jobId = sys.env.getOrElse("SERVERLESS_EMR_JOB_ID", "unknown")
    val applicationId = sys.env.getOrElse("SERVERLESS_EMR_VIRTUAL_CLUSTER_ID", "unknown")

    if (wait.equalsIgnoreCase("streaming")) {
      logInfo(s"""streaming query ${query}""")
      val result = executeQuery(spark, query, dataSource, "", "")
      writeDataFrameToOpensearch(result, resultIndex, osClient)
      spark.streams.awaitAnyTermination()
    } else {
      // Read the values from the Spark configuration or fall back to the default values
      val inactivityLimitMillis: Long =
        conf.getLong("spark.flint.job.inactivityLimitMillis", DEFAULT_INACTIVITY_LIMIT_MILLIS)
      val queryExecutionTimeoutSecs: Duration = Duration(
        conf.getLong(
          "spark.flint.job.queryExecutionTimeoutSec",
          DEFAULT_QUERY_EXECUTION_TIMEOUT.toSeconds),
        SECONDS)
      val queryWaitTimeoutMillis: Long =
        conf.getLong("spark.flint.job.queryWaitTimeoutMillis", DEFAULT_QUERY_WAIT_TIMEOUT_MILLIS)

      val flintSessionIndexUpdater = osClient.createUpdater(sessionIndex.get)
      createShutdownHook(flintSessionIndexUpdater, osClient, sessionIndex.get, sessionId.get)
      // 1 thread for updating heart beat
      val threadPool = ThreadUtils.newDaemonThreadPoolScheduledExecutor("flint-repl-heartbeat", 1)
      val jobStartTime = currentTimeProvider.currentEpochMillis()

      try {
        // update heart beat every 30 seconds
        // OpenSearch triggers recovery after 1 minute outdated heart beat
        createHeartBeatUpdater(
          HEARTBEAT_INTERVAL_MILLIS,
          flintSessionIndexUpdater,
          sessionId.get,
          threadPool,
          osClient,
          sessionIndex.get)

        if (setupFlintJobWithExclusionCheck(
            conf,
            sessionIndex,
            sessionId,
            osClient,
            jobId,
            applicationId,
            flintSessionIndexUpdater,
            jobStartTime)) {
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
            sessionIndex.get)
      } finally {
        spark.stop()
        if (threadPool != null) {
          threadPool.shutdown()
        }
      }
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
    val confExcludeJobsOpt = conf.getOption("spark.flint.deployment.excludeJobs")

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
          // Edge case, current job is excluded, exit the application
          return true
        }

        val getResponse = osClient.getDoc(sessionIndex.get, sessionId.get)
        if (getResponse.isExists()) {
          val source = getResponse.getSourceAsMap
          if (source != null) {
            val existingExcludedJobIds = parseExcludedJobIds(source)
            if (excludeJobIds.sorted == existingExcludedJobIds.sorted) {
              // Edge case, duplicate job running, exit the application
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
    try {
      val futureMappingCheck = Future {
        checkAndCreateIndex(commandContext.osClient, commandContext.resultIndex)
      }

      var lastActivityTime = currentTimeProvider.currentEpochMillis()
      var verificationResult: VerificationResult = NotVerified
      var canPickUpNextStatement = true
      while (currentTimeProvider
          .currentEpochMillis() - lastActivityTime <= commandContext.inactivityLimitMillis && canPickUpNextStatement) {
        logDebug(s"""read from ${commandContext.sessionIndex}""")
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
            executionContext)
          val result: (Long, VerificationResult, Boolean) =
            processCommands(commandContext, commandState)

          val (
            updatedLastActivityTime,
            updatedVerificationResult,
            updatedCanPickUpNextStatement) = result

          lastActivityTime = updatedLastActivityTime
          verificationResult = updatedVerificationResult
          canPickUpNextStatement = updatedCanPickUpNextStatement
        } finally {
          flintReader.close()
        }

        Thread.sleep(100)
      }
    } finally {
      if (threadPool != null) {
        threadPool.shutdown()
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
    val flintJob =
      new FlintInstance(
        applicationId,
        jobId,
        sessionId,
        "running",
        currentTimeProvider.currentEpochMillis(),
        jobStartTime,
        excludeJobIds)
    flintSessionIndexUpdater.upsert(
      sessionId,
      FlintInstance.serialize(flintJob, currentTimeProvider.currentEpochMillis()))
    logDebug(
      s"""Updated job: {"jobid": ${flintJob.jobId}, "sessionId": ${flintJob.sessionId}} from $sessionIndex""")
  }

  def handleSessionError(
      e: Exception,
      applicationId: String,
      jobId: String,
      sessionId: String,
      jobStartTime: Long,
      flintSessionIndexUpdater: OpenSearchUpdater,
      osClient: OSClient,
      sessionIndex: String): Unit = {
    val error = s"Session error: ${e.getMessage}"
    logError(error, e)

    val flintInstance = getExistingFlintInstance(osClient, sessionIndex, sessionId)
      .getOrElse(createFailedFlintInstance(applicationId, jobId, sessionId, jobStartTime, error))

    updateFlintInstance(flintInstance, flintSessionIndexUpdater, sessionId)
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
      FlintInstance.serialize(flintInstance, currentTime))
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
      state: CommandState): (Long, VerificationResult, Boolean) = {
    import context._
    import state._

    var lastActivityTime = recordedLastActivityTime
    var verificationResult = recordedVerificationResult
    var canProceed = true
    var canPickNextStatementResult = true // Add this line to keep track of canPickNextStatement

    while (canProceed) {
      if (!canPickNextStatement(sessionId, jobId, osClient, sessionIndex)) {
        canPickNextStatementResult = false
        canProceed = false
      } else if (!flintReader.hasNext) {
        canProceed = false
      } else {
        lastActivityTime = currentTimeProvider.currentEpochMillis()
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
          osClient)
      }
    }

    // return tuple indicating if still active and mapping verification result
    (lastActivityTime, verificationResult, canPickNextStatementResult)
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
      osClient: OSClient): Unit = {
    try {
      dataToWrite.foreach(df => writeDataFrameToOpensearch(df, resultIndex, osClient))
      if (flintCommand.isRunning() || flintCommand.isWaiting()) {
        // we have set failed state in exception handling
        flintCommand.complete()
      }
      update(flintCommand, flintSessionIndexUpdater)
    } catch {
      // e.g., maybe due to authentication service connection issue
      // or invalid catalog (e.g., we are operating on data not defined in provided data source)
      case e: Exception =>
        val error = s"""Fail to write result of ${flintCommand}, cause: ${e.getMessage}"""
        logError(error, e)
        flintCommand.fail()
        update(flintCommand, flintSessionIndexUpdater)
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

    logDebug(s"command complete: $flintCommand")
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
    update(flintCommand, flintSessionIndexUpdater)
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

    val flintReader = osClient.createReader(sessionIndex, dsl, "submitTime")
    flintReader
  }

  def createShutdownHook(
      flintSessionIndexUpdater: OpenSearchUpdater,
      osClient: OSClient,
      sessionIndex: String,
      sessionId: String,
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
      if (state.isDefined && state.get != "dead" && state.get != "fail") {
        updateFlintInstanceBeforeShutdown(
          source,
          getResponse,
          flintSessionIndexUpdater,
          sessionId)
      }
    })
  }

  private def updateFlintInstanceBeforeShutdown(
      source: java.util.Map[String, AnyRef],
      getResponse: GetResponse,
      flintSessionIndexUpdater: OpenSearchUpdater,
      sessionId: String): Unit = {
    val flintInstance = FlintInstance.deserializeFromMap(source)
    flintInstance.state = "dead"

    flintSessionIndexUpdater.updateIf(
      sessionId,
      FlintInstance.serialize(flintInstance, currentTimeProvider.currentEpochMillis()),
      getResponse.getSeqNo,
      getResponse.getPrimaryTerm)
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
   */
  def createHeartBeatUpdater(
      currentInterval: Long,
      flintSessionUpdater: OpenSearchUpdater,
      sessionId: String,
      threadPool: ScheduledExecutorService,
      osClient: OSClient,
      sessionIndex: String): Unit = {

    threadPool.scheduleAtFixedRate(
      new Runnable {
        override def run(): Unit = {
          try {
            val getResponse = osClient.getDoc(sessionIndex, sessionId)
            if (getResponse.isExists()) {
              val source = getResponse.getSourceAsMap
              val flintInstance = FlintInstance.deserializeFromMap(source)
              flintInstance.state = "running"
              flintSessionUpdater.updateIf(
                sessionId,
                FlintInstance.serialize(flintInstance, currentTimeProvider.currentEpochMillis()),
                getResponse.getSeqNo,
                getResponse.getPrimaryTerm)
            }
            // do nothing if the session doc does not exist
          } catch {
            // maybe due to invalid sequence number or primary term
            case e: Exception =>
              logWarning(
                s"""Fail to update the last update time of the flint instance ${sessionId}""",
                e)
          }
        }
      },
      0L,
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
}
