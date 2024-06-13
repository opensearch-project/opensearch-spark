/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql

import java.util.concurrent.atomic.AtomicInteger

import com.codahale.metrics.Timer
import org.opensearch.flint.core.logging.CustomLogging
import org.opensearch.flint.core.metrics.MetricConstants
import org.opensearch.flint.core.metrics.MetricsUtil.{getTimerContext, incrementCounter, registerGauge, stopTimer}
import org.opensearch.flint.core.storage.{FlintReader, OpenSearchUpdater}
import org.opensearch.flint.data.FlintCommand
import org.opensearch.search.sort.SortOrder

import org.apache.spark.internal.Logging

class CommandLifecycleManagerImpl(commandContext: CommandContext)
    extends CommandLifecycleManager
    with FlintJobExecutor
    with Logging {
  import commandContext._

  private var commandExecutionResultWriter: Option[REPLWriter] = None

  private val statementRunningCount = new AtomicInteger(0)
  registerGauge(MetricConstants.STATEMENT_RUNNING_METRIC, statementRunningCount)

  private val statementTimerContext = getTimerContext(
    MetricConstants.STATEMENT_PROCESSING_TIME_METRIC)

  private val sessionMetadata = sessionManager.getSessionManagerMetadata
  private val flintReader: FlintReader = createQueryReader(
    sessionId = sessionId,
    sessionIndex = sessionMetadata("sessionIndex").asInstanceOf[Option[String]].getOrElse(""),
    dataSource = dataSource)

  val osClient = sessionMetadata("osClient").asInstanceOf[OSClient]
  val flintSessionIndexUpdater =
    sessionMetadata("flintSessionIndexUpdater").asInstanceOf[OpenSearchUpdater]

  override def setWriter(writer: REPLWriter): Unit = {
    this.commandExecutionResultWriter = Some(writer)
  }
  override def prepareCommandLifecycle(): Either[String, Unit] = {
    try {
      val existingSchema = osClient.getIndexMetadata(resultIndex)
      if (!isSuperset(existingSchema, resultIndexMapping)) {
        Left(s"The mapping of $resultIndex is incorrect.")
      } else {
        Right(())
      }
    } catch {
      case e: IllegalStateException
          if e.getCause != null &&
            e.getCause.getMessage.contains("index_not_found_exception") =>
        createResultIndex(osClient, resultIndex, resultIndexMapping)
      case e: InterruptedException =>
        val error = s"Interrupted by the main thread: ${e.getMessage}"
        Thread.currentThread().interrupt() // Preserve the interrupt status
        logError(error, e)
        Left(error)
      case e: Exception =>
        val error = s"Failed to verify existing mapping: ${e.getMessage}"
        logError(error, e)
        Left(error)
    }
  }
  override def initCommandLifecycle(sessionId: String): FlintCommand = {
    val command = flintReader.next()
    logDebug(s"raw command: $command")
    val flintCommand = FlintCommand.deserialize(command)
    logDebug(s"command: $flintCommand")
    flintCommand.running()
    logDebug(s"command running: $flintCommand")
    updateCommandDetails(flintCommand)
    statementRunningCount.incrementAndGet()
    flintCommand
  }

  override def closeCommandLifecycle(): Unit = {
    flintReader.close()
  }
  override def hasPendingCommand(sessionId: String): Boolean = {
    flintReader.hasNext
  }
  override def updateCommandDetails(commandDetails: FlintCommand): Unit = {
    flintSessionIndexUpdater.update(
      commandDetails.statementId,
      FlintCommand.serialize(commandDetails))
  }
  private def createQueryReader(sessionId: String, sessionIndex: String, dataSource: String) = {
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
