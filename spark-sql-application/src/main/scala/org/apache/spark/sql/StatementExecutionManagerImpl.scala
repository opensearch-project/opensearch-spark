/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql

import org.opensearch.flint.common.model.FlintStatement
import org.opensearch.flint.core.storage.{FlintReader, OpenSearchUpdater}
import org.opensearch.search.sort.SortOrder

import org.apache.spark.internal.Logging
import org.apache.spark.sql.FlintJob.{checkAndCreateIndex, createResultIndex, isSuperset, resultIndexMapping}
import org.apache.spark.sql.FlintREPL.executeQuery

class StatementExecutionManagerImpl(
    spark: SparkSession,
    sessionId: String,
    dataSource: String,
    context: Map[String, Any])
    extends StatementExecutionManager
    with Logging {

  private val sessionIndex = context("sessionIndex").asInstanceOf[String]
  private val resultIndex = context("resultIndex").asInstanceOf[String]
  private val osClient = context("osClient").asInstanceOf[OSClient]
  private val flintSessionIndexUpdater =
    context("flintSessionIndexUpdater").asInstanceOf[OpenSearchUpdater]

  // Using one reader client within same session will cause concurrency issue.
  // To resolve this move the reader creation and getNextStatement method to mirco-batch level
  private val flintReader = createOpenSearchQueryReader()

  override def prepareStatementExecution(): Either[String, Unit] = {
    checkAndCreateIndex(osClient, resultIndex)
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
      val flintStatement = FlintStatement.deserialize(rawStatement)
      logInfo(s"Next statement to execute: $flintStatement")
      Some(flintStatement)
    } else {
      None
    }
  }

  override def executeStatement(statement: FlintStatement): DataFrame = {
    executeQuery(spark, statement.query, dataSource, statement.queryId, sessionId, false)
  }

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
