/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql

import java.util.concurrent.{ScheduledExecutorService, ScheduledFuture, TimeUnit}

import scala.collection.JavaConverters._

import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.opensearch.action.get.GetResponse
import org.opensearch.flint.app.FlintCommand
import org.opensearch.flint.core.storage.OpenSearchUpdater
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.types.{ArrayType, LongType, NullType, StringType, StructField, StructType}
import org.apache.spark.sql.util.ShutdownHookManagerTrait

class FlintREPLTest
    extends SparkFunSuite
    with MockitoSugar
    with ArgumentMatchersSugar
    with JobMatchers {
  val spark = SparkSession.builder().appName("Test").master("local").getOrCreate()
  // By using a type alias and casting, I can bypass the type checking error.
  type AnyScheduledFuture = ScheduledFuture[_]

  test("createHeartBeatUpdater should update heartbeat correctly") {
    // Mocks
    val flintSessionUpdater = mock[OpenSearchUpdater]
    val osClient = mock[OSClient]
    val threadPool = mock[ScheduledExecutorService]
    val getResponse = mock[GetResponse]
    val scheduledFutureRaw: ScheduledFuture[_] = mock[ScheduledFuture[_]]

    // Mock behaviors
    when(osClient.getDoc(*, *)).thenReturn(getResponse)
    when(getResponse.isExists()).thenReturn(true)
    when(getResponse.getSourceAsMap).thenReturn(
      Map[String, Object](
        "applicationId" -> "app1",
        "jobId" -> "job1",
        "sessionId" -> "session1",
        "lastUpdateTime" -> java.lang.Long.valueOf(12345L),
        "error" -> "someError").asJava)
    when(getResponse.getSeqNo).thenReturn(0L)
    when(getResponse.getPrimaryTerm).thenReturn(0L)
    // when scheduled task is scheduled, execute the runnable immediately only once and become no-op afterwards.
    when(
      threadPool.scheduleAtFixedRate(
        any[Runnable],
        eqTo(0),
        *,
        eqTo(java.util.concurrent.TimeUnit.MILLISECONDS)))
      .thenAnswer((invocation: InvocationOnMock) => {
        val runnable = invocation.getArgument[Runnable](0)
        runnable.run()
        scheduledFutureRaw
      })

    // Invoke the method
    FlintREPL.createHeartBeatUpdater(
      1000L,
      flintSessionUpdater,
      "session1",
      threadPool,
      osClient,
      "sessionIndex")

    // Verifications
    verify(osClient, atLeastOnce()).getDoc("sessionIndex", "session1")
    verify(flintSessionUpdater, atLeastOnce()).updateIf(eqTo("session1"), *, eqTo(0L), eqTo(0L))
  }

  test("createShutdownHook add shutdown hook and update FlintInstance if conditions are met") {
    val flintSessionIndexUpdater = mock[OpenSearchUpdater]
    val osClient = mock[OSClient]
    val getResponse = mock[GetResponse]
    val sessionIndex = "testIndex"
    val sessionId = "testSessionId"

    when(osClient.getDoc(sessionIndex, sessionId)).thenReturn(getResponse)
    when(getResponse.isExists()).thenReturn(true)

    when(getResponse.getSourceAsMap).thenReturn(
      Map[String, Object](
        "applicationId" -> "app1",
        "jobId" -> "job1",
        "sessionId" -> "session1",
        "state" -> "running",
        "lastUpdateTime" -> java.lang.Long.valueOf(12345L),
        "error" -> "someError").asJava)

    val mockShutdownHookManager = new ShutdownHookManagerTrait {
      override def addShutdownHook(hook: () => Unit): AnyRef = {
        hook() // execute the hook immediately
        new Object() // return a dummy AnyRef as per the method signature
      }
    }

    // Here, we're injecting our mockShutdownHookManager into the method
    FlintREPL.createShutdownHook(
      flintSessionIndexUpdater,
      osClient,
      sessionIndex,
      sessionId,
      mockShutdownHookManager)

    verify(flintSessionIndexUpdater).updateIf(*, *, *, *)
  }

  test("Test getFailedData method") {
    // Define expected dataframe
    val dataSourceName = "myGlueS3"
    val expectedSchema = StructType(
      Seq(
        StructField("result", NullType, nullable = true),
        StructField("schema", NullType, nullable = true),
        StructField("jobRunId", StringType, nullable = true),
        StructField("applicationId", StringType, nullable = true),
        StructField("dataSourceName", StringType, nullable = true),
        StructField("status", StringType, nullable = true),
        StructField("error", StringType, nullable = true),
        StructField("queryId", StringType, nullable = true),
        StructField("queryText", StringType, nullable = true),
        StructField("sessionId", StringType, nullable = true),
        StructField("updateTime", LongType, nullable = false),
        StructField("queryRunTime", LongType, nullable = false)))

    val currentTime: Long = System.currentTimeMillis()
    val queryRunTime: Long = 3000L

    val error = "some error"
    val expectedRows = Seq(
      Row(
        null,
        null,
        "unknown",
        "unknown",
        dataSourceName,
        "FAILED",
        error,
        "10",
        "select 1",
        "20",
        currentTime,
        queryRunTime))
    val expected: DataFrame =
      spark.createDataFrame(spark.sparkContext.parallelize(expectedRows), expectedSchema)

    val flintCommand = new FlintCommand("failed", "select 1", "30", "10", currentTime, None)

    FlintREPL.currentTimeProvider = new MockTimeProvider(currentTime)

    // Compare the result
    val result =
      FlintREPL.handleCommandFailureAndGetFailedData(
        spark,
        dataSourceName,
        error,
        flintCommand,
        "20",
        currentTime - queryRunTime)
    assertEqualDataframe(expected, result)
    assert("failed" == flintCommand.state)
    assert(error == flintCommand.error.get)
  }
}
