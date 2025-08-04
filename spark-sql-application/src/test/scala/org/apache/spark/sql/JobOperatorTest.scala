/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql

import java.util.concurrent.atomic.AtomicInteger

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{doNothing, times, verify, when}
import org.opensearch.flint.common.model.FlintStatement
import org.scalatest.BeforeAndAfterEach
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.{SparkContext, SparkFunSuite}
import org.apache.spark.sql.flint.config.FlintSparkConf
import org.apache.spark.sql.streaming.StreamingQueryManager

class JobOperatorTest
    extends SparkFunSuite
    with MockitoSugar
    with Matchers
    with BeforeAndAfterEach {

  private val jobId = "testJobId"
  private val resultIndex = "resultIndex"
  private val jobType = "interactive"
  private val applicationId = "testApplicationId"
  private val dataSource = "testDataSource"

  override def beforeEach(): Unit = {
    super.beforeEach()
  }

  test("verify if statementExecutionManager is calling update during non-warmpool jobs ") {
    try {
      val mockFlintStatement = mock[FlintStatement]
      when(mockFlintStatement.queryId).thenReturn("test-query-id")
      when(mockFlintStatement.query).thenReturn("SELECT 1")
      val streamingRunningCount = new AtomicInteger(1)
      val statementRunningCount = new AtomicInteger(1)
      val mockSparkSession = mock[SparkSession]
      val mockDataFrame = mock[DataFrame]
      val mockSparkConf = mock[RuntimeConfig]

      val mockStatementExecutionManager = mock[StatementExecutionManager]
      val mockOSClient = mock[OSClient]
      val mockSparkContext = mock[SparkContext]
      val mockStreamingQueryManager = mock[StreamingQueryManager]

      when(mockSparkSession.sql("SELECT 1")).thenReturn(mockDataFrame)
      when(mockSparkSession.conf).thenReturn(mockSparkConf)
      when(mockSparkConf.get(FlintSparkConf.WARMPOOL_ENABLED.key, "false"))
        .thenReturn("false")
      when(mockSparkConf.get(FlintSparkConf.REQUEST_INDEX.key, ""))
        .thenReturn(resultIndex)
      when(mockSparkSession.sparkContext).thenReturn(mockSparkContext)
      doNothing().when(mockSparkContext).addSparkListener(any())
      when(mockSparkSession.streams).thenReturn(mockStreamingQueryManager)
      doNothing().when(mockStreamingQueryManager).addListener(any())
      doNothing()
        .when(mockStatementExecutionManager)
        .updateStatement(any[FlintStatement])
      when(mockStatementExecutionManager.prepareStatementExecution())
        .thenReturn(Right(()))
      when(mockStatementExecutionManager.executeStatement(any[FlintStatement]))
        .thenReturn(mockDataFrame)

      when(mockOSClient.doesIndexExist(resultIndex)).thenReturn(true)

      val jobOperator = new JobOperator(
        applicationId,
        jobId,
        mockSparkSession,
        mockFlintStatement,
        dataSource,
        resultIndex,
        jobType,
        streamingRunningCount,
        statementRunningCount) {
        override protected def instantiateStatementExecutionManager(
            commandContext: CommandContext,
            resultIndex: String,
            osClient: OSClient): StatementExecutionManager = {
          mockStatementExecutionManager
        }

        override def writeDataFrameToOpensearch(
            resultData: DataFrame,
            resultIndex: String,
            osClient: OSClient): Unit = {}

        override def instantiateQueryResultWriter(
            spark: SparkSession,
            commandContext: CommandContext): QueryResultWriter = {
          val mockQueryResultWriter = mock[QueryResultWriter]
          when(
            mockQueryResultWriter
              .processDataFrame(any[DataFrame], any[FlintStatement], any[Long]))
            .thenAnswer(invocation => invocation.getArgument[DataFrame](0))
          doNothing()
            .when(mockQueryResultWriter)
            .writeDataFrame(any[DataFrame], any[FlintStatement])
          mockQueryResultWriter
        }

        override def start(): Unit = {
          try {
            if (!isWarmpoolEnabled) {
              mockStatementExecutionManager.updateStatement(mockFlintStatement)
            }

            mockStatementExecutionManager.prepareStatementExecution() match {
              case Right(_) =>
                val data = mockStatementExecutionManager.executeStatement(mockFlintStatement)
                val queryResultWriter =
                  instantiateQueryResultWriter(mockSparkSession, null)
                queryResultWriter.writeDataFrame(data, mockFlintStatement)
              case Left(err) =>
            }

            mockFlintStatement.complete()
            mockStatementExecutionManager.updateStatement(mockFlintStatement)
          } catch {
            case e: Exception =>
              mockFlintStatement.fail()
              mockStatementExecutionManager.updateStatement(mockFlintStatement)
          }
        }
      }

      jobOperator.start()
      verify(mockStatementExecutionManager, times(2)).updateStatement(mockFlintStatement);
    } catch {
      case e: Exception =>
        print("Exception : ", e.printStackTrace())
    }
  }
}
