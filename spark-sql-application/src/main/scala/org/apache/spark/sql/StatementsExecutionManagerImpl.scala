/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql

import org.opensearch.flint.common.model.FlintStatement
import org.opensearch.flint.core.storage.{FlintReader, OpenSearchUpdater}
import org.opensearch.search.sort.SortOrder

import org.apache.spark.internal.Logging
import org.apache.spark.sql.FlintJob.{createResultIndex, isSuperset, resultIndexMapping}
import org.apache.spark.sql.FlintREPL.executeQuery

class StatementsExecutionManagerImpl(
    sessionId: String,
    dataSource: String,
    context: Map[String, Any])
    extends StatementsExecutionManager
    with Logging {

  val sessionIndex = context("sessionIndex").asInstanceOf[String]
  val resultIndex = context("resultIndex").asInstanceOf[String]
  val osClient = context("osClient").asInstanceOf[OSClient]
  val flintSessionIndexUpdater =
    context("flintSessionIndexUpdater").asInstanceOf[OpenSearchUpdater]

  // Using one reader client within same session will cause concurrency issue.
  // To resolve this move the reader creation and getNextStatement method to mirco-batch level
  val flintReader = createOpenSearchQueryReader()

  override def prepareStatementExecution(): Either[String, Unit] = {
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
  override def updateStatement(statement: FlintStatement): Unit = {
    flintSessionIndexUpdater.update(statement.statementId, FlintStatement.serialize(statement))
  }
  override def terminateStatementsExecution(): Unit = {
    flintReader.close()
  }

  override def getNextStatement(): Option[FlintStatement] = {
    if (flintReader.hasNext) {
      val rawStatement = flintReader.next()
      logInfo(s"raw statement: $rawStatement")
      val flintStatement = FlintStatement.deserialize(rawStatement)
      logInfo(s"statement: $flintStatement")
      Some(flintStatement)
    } else {
      None
    }
  }

  //  override def executeStatement(statement: FlintStatement): DataFrame = {
//    executeQuery()
//  }

  private def createOpenSearchQueryReader() = {
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
}
