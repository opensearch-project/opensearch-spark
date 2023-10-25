/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql

import java.time.Instant
import java.util.concurrent.{ScheduledExecutorService, ThreadPoolExecutor}

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future, TimeoutException}
import scala.concurrent.duration.{Duration, MINUTES, SECONDS}

import org.opensearch.action.get.GetResponse
import org.opensearch.common.Strings
import org.opensearch.flint.app.{FlintCommand, FlintInstance}
import org.opensearch.flint.app.FlintCommand.serialize
import org.opensearch.flint.core.storage.{FlintReader, OpenSearchUpdater}

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.FlintJob.{currentTimeProvider, getFailedData, writeData}
import org.apache.spark.sql.FlintREPL.executeQuery
import org.apache.spark.sql.flint.config.FlintSparkConf
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}
import org.apache.spark.sql.util.{DefaultShutdownHookManager, ShutdownHookManagerTrait}
import org.apache.spark.util.{ShutdownHookManager, ThreadUtils}

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
  private val INACTIVITY_LIMIT_MILLIS = 30 * 60 * 1000
  private val MAPPING_CHECK_TIMEOUT = Duration(1, MINUTES)
  private val QUERY_EXECUTION_TIMEOUT = Duration(10, MINUTES)
  private val QUERY_WAIT_TIMEOUT_MILLIS = 10 * 60 * 1000

  def update(flintCommand: FlintCommand, updater: OpenSearchUpdater): Unit = {
    updater.update(flintCommand.statementId, serialize(flintCommand))
  }

  def main(args: Array[String]) {
    val Array(query, resultIndex) = args
    if (Strings.isNullOrEmpty(resultIndex)) {
      throw new IllegalArgumentException("resultIndex is not set")
    }

    // init SparkContext
    val conf: SparkConf = createSparkConf()
    val dataSource = conf.get("spark.flint.datasource.name", "unknown")
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
      writeData(result, resultIndex)
      spark.streams.awaitAnyTermination()
    } else {
      try {
        val flintSessionIndexUpdater = osClient.createUpdater(sessionIndex.get)
        createShutdownHook(flintSessionIndexUpdater, osClient, sessionIndex.get, sessionId.get);

        queryLoop(
          resultIndex,
          dataSource,
          sessionIndex.get,
          sessionId.get,
          spark,
          osClient,
          jobId,
          applicationId,
          flintSessionIndexUpdater)
      } finally {
        spark.stop()
      }
    }
  }

  private def queryLoop(
      resultIndex: String,
      dataSource: String,
      sessionIndex: String,
      sessionId: String,
      spark: SparkSession,
      osClient: OSClient,
      jobId: String,
      applicationId: String,
      flintSessionIndexUpdater: OpenSearchUpdater): Unit = {
    // 1 thread for updating heart beat
    val threadPool = ThreadUtils.newDaemonThreadPoolScheduledExecutor("flint-repl", 1)
    implicit val executionContext = ExecutionContext.fromExecutor(threadPool)

    try {
      val futureMappingCheck = Future {
        checkAndCreateIndex(osClient, resultIndex)
      }

      setupFlintJob(applicationId, jobId, sessionId, flintSessionIndexUpdater, sessionIndex)

      // update heart beat every 30 seconds
      // OpenSearch triggers recovery after 1 minute outdated heart beat
      createHeartBeatUpdater(
        HEARTBEAT_INTERVAL_MILLIS,
        flintSessionIndexUpdater,
        sessionId: String,
        threadPool,
        osClient,
        sessionIndex)

      var lastActivityTime = Instant.now().toEpochMilli()
      var verificationResult: VerificationResult = NotVerified

      while (Instant.now().toEpochMilli() - lastActivityTime <= INACTIVITY_LIMIT_MILLIS) {
        logInfo(s"""read from ${sessionIndex}""")
        val flintReader: FlintReader =
          createQueryReader(osClient, applicationId, sessionId, sessionIndex, dataSource)

        try {
          val result: (Long, VerificationResult) = processCommands(
            flintReader,
            spark,
            dataSource,
            resultIndex,
            sessionId,
            futureMappingCheck,
            verificationResult,
            executionContext,
            flintSessionIndexUpdater,
            lastActivityTime)

          val (updatedLastActivityTime, updatedVerificationResult) = result

          lastActivityTime = updatedLastActivityTime
          verificationResult = updatedVerificationResult
        } finally {
          flintReader.close()
        }

        Thread.sleep(100)
      }

    } catch {
      case e: Exception =>
        handleSessionError(e, applicationId, jobId, sessionId, flintSessionIndexUpdater)
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
      sessionIndex: String): Unit = {
    val flintJob =
      new FlintInstance(applicationId, jobId, sessionId, "running", System.currentTimeMillis())
    flintSessionIndexUpdater.upsert(sessionId, FlintInstance.serialize(flintJob))
    logInfo(
      s"""Updated job: {"jobid": ${flintJob.jobId}, "sessionId": ${flintJob.sessionId}} from $sessionIndex""")
  }

  private def handleSessionError(
      e: Exception,
      applicationId: String,
      jobId: String,
      sessionId: String,
      flintSessionIndexUpdater: OpenSearchUpdater): Unit = {
    val error = s"Unexpected error: ${e.getMessage}"
    logError(error, e)
    val flintJob = new FlintInstance(
      applicationId,
      jobId,
      sessionId,
      "fail",
      System.currentTimeMillis(),
      Some(error))
    flintSessionIndexUpdater.upsert(sessionId, FlintInstance.serialize(flintJob))
  }

  /**
   * handling the case where a command's execution fails, updates the flintCommand with the error
   * and failure status, and then delegates to the second method for actual DataFrame creation
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
      flintReader: FlintReader,
      spark: SparkSession,
      dataSource: String,
      resultIndex: String,
      sessionId: String,
      futureMappingCheck: Future[Either[String, Unit]],
      recordedVerificationResult: VerificationResult,
      executionContext: ExecutionContextExecutor,
      flintSessionIndexUpdater: OpenSearchUpdater,
      recordedLastActivityTime: Long): (Long, VerificationResult) = {
    var lastActivityTime = recordedLastActivityTime
    var verificationResult = recordedVerificationResult

    while (flintReader.hasNext) {
      lastActivityTime = Instant.now().toEpochMilli()
      val flintCommand = processCommandInitiation(flintReader, flintSessionIndexUpdater)

      spark.sparkContext.setJobGroup(
        flintCommand.queryId,
        "Job group for " + flintCommand.queryId,
        interruptOnCancel = true)
      val (dataToWrite, returnedVerificationResult) = processStatementOnVerification(
        recordedVerificationResult,
        spark,
        flintCommand,
        dataSource,
        sessionId,
        executionContext,
        futureMappingCheck,
        resultIndex)

      verificationResult = returnedVerificationResult
      finalizeCommand(dataToWrite, flintCommand, resultIndex, flintSessionIndexUpdater)
    }

    // return tuple indicating if still active and mapping verification result
    (lastActivityTime, verificationResult)
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
      flintSessionIndexUpdater: OpenSearchUpdater): Unit = {
    try {
      dataToWrite.foreach(df => writeData(df, resultIndex))
      if (flintCommand.isRunning()) {
        // we have set failed state in exception handling
        flintCommand.complete()
      }
      update(flintCommand, flintSessionIndexUpdater)
    } catch {
      // e.g., maybe due to authentication service connection issue
      case e: Exception =>
        val error = s"""Fail to write result of ${flintCommand}, cause: ${e.getMessage}"""
        logError(error, e)
        flintCommand.fail()
        update(flintCommand, flintSessionIndexUpdater)
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
      resultIndex: String): (Option[DataFrame], VerificationResult) = {
    val startTime: Long = System.currentTimeMillis()
    var verificationResult = recordedVerificationResult
    var dataToWrite: Option[DataFrame] = None
    try {
      verificationResult match {
        case NotVerified =>
          try {
            val mappingCheckResult =
              ThreadUtils.awaitResult(futureMappingCheck, MAPPING_CHECK_TIMEOUT)
            // time out after 10 minutes
            val result = executeQueryAsync(
              spark,
              flintCommand,
              dataSource,
              sessionId,
              executionContext,
              startTime)

            dataToWrite = Some(mappingCheckResult match {
              case Right(_) =>
                verificationResult = VerifiedWithoutError
                result
              case Left(error) =>
                verificationResult = VerifiedWithError(error)
                handleCommandFailureAndGetFailedData(
                  spark,
                  dataSource,
                  error,
                  flintCommand,
                  sessionId,
                  startTime)
            })
          } catch {
            case e: TimeoutException =>
              val error = s"Getting the mapping of index $resultIndex timed out"
              logError(error, e)
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
          dataToWrite = Some(
            executeQueryAsync(
              spark,
              flintCommand,
              dataSource,
              sessionId,
              executionContext,
              startTime))
      }

      logDebug(s"""command complete: ${flintCommand}""")
    } catch {
      case e: TimeoutException =>
        val error = s"Executing ${flintCommand.query} timed out"
        spark.sparkContext.cancelJobGroup(flintCommand.queryId)
        logError(error, e)
        dataToWrite = Some(
          handleCommandFailureAndGetFailedData(
            spark,
            dataSource,
            error,
            flintCommand,
            sessionId,
            startTime))
      case e: Exception =>
        val error = processQueryException(e, spark, dataSource, flintCommand, sessionId)
        dataToWrite = Some(
          handleCommandFailureAndGetFailedData(
            spark,
            dataSource,
            error,
            flintCommand,
            sessionId,
            startTime))
    }
    (dataToWrite, verificationResult)
  }

  def executeQueryAsync(
      spark: SparkSession,
      flintCommand: FlintCommand,
      dataSource: String,
      sessionId: String,
      executionContext: ExecutionContextExecutor,
      startTime: Long): DataFrame = {
    if (Instant.now().toEpochMilli() - flintCommand.submitTime > QUERY_WAIT_TIMEOUT_MILLIS) {
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
      ThreadUtils.awaitResult(futureQueryExecution, QUERY_EXECUTION_TIMEOUT)
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
      applicationId: String,
      sessionId: String,
      sessionIndex: String,
      dataSource: String) = {
    // all state in index are in lower case
    //
    // add application to deal with emr-s deployment:
    // Should the EMR-S application be terminated or halted (e.g., during deployments), OpenSearch associates each
    // query to ascertain the latest EMR-S application ID. While existing EMR-S jobs continue executing queries sharing
    // the same applicationId and sessionId, new requests with differing application IDs will not be processed by the
    // current EMR-S job. Gradually, the queue with the same applicationId and sessionId diminishes, enabling the
    // job to self-terminate due to inactivity timeout. Following the termination of all jobs, the CP can gracefully
    // shut down the application. The session has a phantom state where the old application set the state to dead and
    // then the new application job sets it to running through heartbeat. Also, application id can help in the case
    // of job restart where the job id is different but application id is the same.
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
         |          "applicationId": "$applicationId"
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
    val flintInstant = new FlintInstance(
      source.get("applicationId").asInstanceOf[String],
      source.get("jobId").asInstanceOf[String],
      source.get("sessionId").asInstanceOf[String],
      "dead",
      source.get("lastUpdateTime").asInstanceOf[Long],
      Option(source.get("error").asInstanceOf[String]))

    flintSessionIndexUpdater.updateIf(
      sessionId,
      FlintInstance.serialize(flintInstant),
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
              val flintInstant: FlintInstance = new FlintInstance(
                source.get("applicationId").asInstanceOf[String],
                source.get("jobId").asInstanceOf[String],
                source.get("sessionId").asInstanceOf[String],
                "running",
                source.get("lastUpdateTime").asInstanceOf[Long],
                Option(source.get("error").asInstanceOf[String]))
              flintSessionUpdater.updateIf(
                sessionId,
                FlintInstance.serialize(flintInstant),
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
}
