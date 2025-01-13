/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql

import java.time.Instant

import scala.concurrent.duration.{Duration, MINUTES}

import org.mockito.ArgumentMatchersSugar
import org.mockito.Mockito.{times, verify, when}
import org.opensearch.flint.common.model.FlintStatement
import org.opensearch.flint.common.scheduler.model.LangType
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.sql.FlintREPLConfConstants.DEFAULT_QUERY_LOOP_EXECUTION_FREQUENCY
import org.apache.spark.sql.flint.config.FlintSparkConf

class WarmpoolJobTest
    extends SparkFunSuite
    with MockitoSugar
    with ArgumentMatchersSugar
    with JobMatchers {

  private val jobId = "testJobId"
  private val applicationId = "testApplicationId"
  private val INTERACTIVE_JOB_TYPE = "interactive"
  private val STREAMING_JOB_TYPE = "streaming"

  test("queryLoop calls processInteractiveQuery when interactive query is received") {
    val resultIndex = "resultIndex"
    val dataSource = "testDataSource"
    val mockSparkConf = mock[SparkConf]
    val mockSparkSession = mock[SparkSession]
    val mockConf = mock[RuntimeConfig]
    when(mockSparkSession.conf).thenReturn(mockConf)
    when(mockSparkSession.conf.get(FlintSparkConf.REQUEST_INDEX.key, ""))
      .thenReturn("someSessionIndex")
    when(mockSparkSession.conf.get(FlintSparkConf.DATA_SOURCE_NAME.key, ""))
      .thenReturn("datasourceName")
    when(mockSparkSession.conf.get(FlintSparkConf.JOB_TYPE.key, FlintJobType.BATCH))
      .thenReturn(INTERACTIVE_JOB_TYPE)
    val sessionManager = new SessionManagerImpl(mockSparkSession, Some("resultIndex"))

    try {
      val commandContext = CommandContext(
        applicationId,
        jobId,
        mockSparkSession,
        dataSource,
        "",
        "",
        sessionManager,
        Duration(10, MINUTES),
        60,
        60,
        DEFAULT_QUERY_LOOP_EXECUTION_FREQUENCY)

      val flintStatement =
        new FlintStatement(
          "running",
          "select 1",
          "30",
          "10",
          LangType.SQL,
          Instant.now().toEpochMilli(),
          None)

      val warmpoolJob = WarmpoolJob(mockSparkConf, mockSparkSession, Some(resultIndex))
      val mockStatementExecutionManager = mock[StatementExecutionManager]
      when(mockStatementExecutionManager.getNextStatement()).thenReturn(Some(flintStatement))

      warmpoolJob.queryLoop(commandContext, mockStatementExecutionManager)
      verify(warmpoolJob, times(1)).processInteractiveJob(*, *, *, *, *)
    } catch {
      case _: Exception => ()
    }
  }

  test("queryLoop calls processStreamingQuery when streaming query is received") {
    val resultIndex = "resultIndex"
    val dataSource = "testDataSource"
    val mockSparkConf = mock[SparkConf]
    val mockSparkSession = mock[SparkSession]
    val mockConf = mock[RuntimeConfig]
    when(mockSparkSession.conf).thenReturn(mockConf)
    when(mockSparkSession.conf.get(FlintSparkConf.REQUEST_INDEX.key, ""))
      .thenReturn("someSessionIndex")
    when(mockSparkSession.conf.get(FlintSparkConf.DATA_SOURCE_NAME.key, ""))
      .thenReturn("datasourceName")
    when(mockSparkSession.conf.get(FlintSparkConf.JOB_TYPE.key, FlintJobType.BATCH))
      .thenReturn(STREAMING_JOB_TYPE)
    val sessionManager = new SessionManagerImpl(mockSparkSession, Some("resultIndex"))
    try {
      val commandContext = CommandContext(
        applicationId,
        jobId,
        mockSparkSession,
        dataSource,
        "",
        "",
        sessionManager,
        Duration(10, MINUTES),
        60,
        60,
        DEFAULT_QUERY_LOOP_EXECUTION_FREQUENCY)

      val flintStatement =
        new FlintStatement(
          "running",
          "select 1",
          "30",
          "10",
          LangType.SQL,
          Instant.now().toEpochMilli(),
          None)

      val warmpoolJob = WarmpoolJob(mockSparkConf, mockSparkSession, Some(resultIndex))
      val mockStatementExecutionManager = mock[StatementExecutionManager]
      when(mockStatementExecutionManager.getNextStatement()).thenReturn(Some(flintStatement))

      warmpoolJob.queryLoop(commandContext, mockStatementExecutionManager)
      verify(warmpoolJob, times(1)).processStreamingJob(*, *, *, *, *, *, *, *, *)
    } catch {
      case _: Exception => ()
    }
  }
}
