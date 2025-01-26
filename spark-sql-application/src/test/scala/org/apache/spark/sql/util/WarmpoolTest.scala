/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql

import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger

import org.mockito.ArgumentMatchers.{any, anyString}
import org.mockito.Mockito.{doAnswer, spy, times, verify, when}
import org.opensearch.flint.common.model.FlintStatement
import org.opensearch.flint.common.scheduler.model.LangType
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.flint.config.FlintSparkConf

class WarmpoolTest extends SparkFunSuite with MockitoSugar with JobMatchers {
  private val jobId = "testJobId"
  private val applicationId = "testApplicationId"
  val streamingRunningCount = new AtomicInteger(0)
  val statementRunningCount = new AtomicInteger(0)
  var mockStatementExecutionManager: StatementExecutionManager = _
  val resultIndex = "testResultIndex"
  val dataSourceName = "my_glue1"
  val requestIndex = "testRequestIndex"

  test("verify job operator starts twice when there are two Flint statements") {
    val mockSparkSession = mock[SparkSession]
    val mockConf = mock[RuntimeConfig]
    val mockStatementExecutionManager = mock[StatementExecutionManager]
    val mockJobOperator = mock[JobOperator]

    val firstFlintStatement = new FlintStatement(
      "waiting",
      "select 1",
      "30",
      "10",
      LangType.SQL,
      Instant.now().toEpochMilli(),
      None)

    val secondFlintStatement = new FlintStatement(
      "waiting",
      "select * from DB",
      "30",
      "10",
      LangType.SQL,
      Instant.now().toEpochMilli(),
      None)

    when(mockSparkSession.conf).thenReturn(mockConf)
    when(mockStatementExecutionManager.getNextStatement())
      .thenReturn(Some(firstFlintStatement))
      .thenReturn(Some(secondFlintStatement))
      .thenReturn(None)

    when(mockSparkSession.conf.get(FlintSparkConf.JOB_TYPE.key, FlintJobType.BATCH))
      .thenReturn(FlintJobType.BATCH)
    when(mockSparkSession.conf.get(FlintSparkConf.DATA_SOURCE_NAME.key))
      .thenReturn(dataSourceName)
    when(mockSparkSession.conf.get(FlintSparkConf.RESULT_INDEX.key)).thenReturn(resultIndex)
    when(mockSparkSession.conf.get(FlintSparkConf.TERMINATE_JVM.key, "true")).thenReturn("true")
    when(mockSparkSession.conf.get(FlintSparkConf.WARMPOOL_ENABLED.key, "false"))
      .thenReturn("true")
    when(mockSparkSession.conf.get(FlintSparkConf.REQUEST_INDEX.key, ""))
      .thenReturn(requestIndex)

    val job = spy(
      WarmpoolJob(
        applicationId,
        jobId,
        mockSparkSession,
        statementRunningCount,
        statementRunningCount))

    doAnswer(_ => mockJobOperator)
      .when(job)
      .createJobOperator(any(), anyString(), anyString(), anyString())

    job.queryLoop(mockStatementExecutionManager)
    verify(mockJobOperator, times(2)).start()
  }

  test("Query loop execution failure") {
    val mockSparkSession = mock[SparkSession]
    val mockConf = mock[RuntimeConfig]
    val mockStatementExecutionManager = mock[StatementExecutionManager]

    when(mockSparkSession.conf).thenReturn(mockConf)
    when(mockStatementExecutionManager.getNextStatement())
      .thenThrow(new RuntimeException("something went wrong"))

    val job =
      WarmpoolJob(
        applicationId,
        jobId,
        mockSparkSession,
        statementRunningCount,
        statementRunningCount)

    assertThrows[Throwable] {
      job.queryLoop(mockStatementExecutionManager)
    }
  }
}
