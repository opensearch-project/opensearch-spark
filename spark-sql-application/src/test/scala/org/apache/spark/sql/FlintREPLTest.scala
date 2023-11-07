/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql

import java.net.ConnectException
import java.time.Instant
import java.util.concurrent.{ScheduledExecutorService, ScheduledFuture, TimeUnit}

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, TimeoutException}
import scala.concurrent.duration._
import scala.reflect.runtime.universe.TypeTag

import org.mockito.ArgumentMatchersSugar
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.opensearch.action.get.GetResponse
import org.opensearch.flint.app.FlintCommand
import org.opensearch.flint.core.storage.{OpenSearchReader, OpenSearchUpdater}
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.{SparkContext, SparkFunSuite}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.trees.Origin
import org.apache.spark.sql.types.{ArrayType, LongType, NullType, StringType, StructField, StructType}
import org.apache.spark.sql.util.ShutdownHookManagerTrait
import org.apache.spark.util.ThreadUtils

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

  test("test canPickNextStatement: Doc Exists and Valid JobId") {
    val sessionId = "session123"
    val jobId = "jobABC"
    val osClient = mock[OSClient]
    val sessionIndex = "sessionIndex"

    val getResponse = mock[GetResponse]
    when(osClient.getDoc(sessionIndex, sessionId)).thenReturn(getResponse)
    when(getResponse.isExists()).thenReturn(true)

    val sourceMap: java.util.Map[String, Object] = new java.util.HashMap[String, Object]()
    sourceMap.put("jobId", jobId.asInstanceOf[Object])
    when(getResponse.getSourceAsMap).thenReturn(sourceMap)

    val result = FlintREPL.canPickNextStatement(sessionId, jobId, osClient, sessionIndex)

    assert(result)
  }

  test("test canPickNextStatement: Doc Exists but Different JobId") {
    val sessionId = "session123"
    val jobId = "jobABC"
    val differentJobId = "jobXYZ"
    val osClient = mock[OSClient]
    val sessionIndex = "sessionIndex"

    val getResponse = mock[GetResponse]
    when(osClient.getDoc(sessionIndex, sessionId)).thenReturn(getResponse)
    when(getResponse.isExists()).thenReturn(true)

    val sourceMap: java.util.Map[String, Object] = new java.util.HashMap[String, Object]()
    sourceMap.put("jobId", differentJobId.asInstanceOf[Object])
    when(getResponse.getSourceAsMap).thenReturn(sourceMap)

    // Execute the method under test
    val result = FlintREPL.canPickNextStatement(sessionId, jobId, osClient, sessionIndex)

    // Assertions
    assert(!result) // The function should return false
  }

  test("test canPickNextStatement: Doc Exists, JobId Matches, but JobId is Excluded") {
    val sessionId = "session123"
    val jobId = "jobABC"
    val osClient = mock[OSClient]
    val sessionIndex = "sessionIndex"

    val getResponse = mock[GetResponse]
    when(osClient.getDoc(sessionIndex, sessionId)).thenReturn(getResponse)
    when(getResponse.isExists()).thenReturn(true)

    val excludeJobIdsList = new java.util.ArrayList[String]()
    excludeJobIdsList.add(jobId) // Add the jobId to the list to simulate exclusion

    val sourceMap: java.util.Map[String, Object] = new java.util.HashMap[String, Object]()
    sourceMap.put("jobId", jobId) // The jobId matches
    sourceMap.put("excludeJobIds", excludeJobIdsList) // But jobId is in the exclude list
    when(getResponse.getSourceAsMap).thenReturn(sourceMap)

    // Execute the method under test
    val result = FlintREPL.canPickNextStatement(sessionId, jobId, osClient, sessionIndex)

    // Assertions
    assert(!result) // The function should return false because jobId is excluded
  }

  test("test canPickNextStatement: Doc Exists but Source is Null") {
    val sessionId = "session123"
    val jobId = "jobABC"
    val osClient = mock[OSClient]
    val sessionIndex = "sessionIndex"

    // Mock the getDoc response
    val getResponse = mock[GetResponse]
    when(osClient.getDoc(sessionIndex, sessionId)).thenReturn(getResponse)
    when(getResponse.isExists()).thenReturn(true)
    when(getResponse.getSourceAsMap).thenReturn(null) // Simulate the source being null

    // Capture the logs if your logger allows it or redirect System.out for System.log calls
    // Assuming `logError` is the method you would call when the source is null

    // Execute the method under test
    val result = FlintREPL.canPickNextStatement(sessionId, jobId, osClient, sessionIndex)

    // Assertions
    assert(result) // The function should return true despite the null source
  }

  test("test canPickNextStatement: Doc Exists with Unexpected Type in excludeJobIds") {
    val sessionId = "session123"
    val jobId = "jobABC"
    val osClient = mock[OSClient]
    val sessionIndex = "sessionIndex"

    val getResponse = mock[GetResponse]
    when(osClient.getDoc(sessionIndex, sessionId)).thenReturn(getResponse)
    when(getResponse.isExists()).thenReturn(true)

    val sourceMap: java.util.Map[String, Object] = new java.util.HashMap[String, Object]()
    sourceMap.put("jobId", jobId)
    sourceMap.put(
      "excludeJobIds",
      Integer.valueOf(123)
    ) // Using an Integer here to represent an unexpected type

    when(getResponse.getSourceAsMap).thenReturn(sourceMap)

    // Assuming logInfo is used for logging unexpected types and that it is a method on FlintREPL
    // You would have a mock or a way to verify that logInfo was called with the expected message
    // If you're using a logging framework that supports appending log messages to a list for tests,
    // like logback-test.xml configuration with a ListAppender, set it up here.

    val result = FlintREPL.canPickNextStatement(sessionId, jobId, osClient, sessionIndex)

    assert(result) // The function should return true
  }

  test("test canPickNextStatement: Doc Does Not Exist") {
    val sessionId = "session123"
    val jobId = "jobABC"
    val osClient = mock[OSClient]
    val sessionIndex = "sessionIndex"

    // Set up the mock GetResponse
    val getResponse = mock[GetResponse]
    when(osClient.getDoc(sessionIndex, sessionId)).thenReturn(getResponse)
    when(getResponse.isExists()).thenReturn(false) // Simulate the document does not exist

    // Redirect or mock the logger as appropriate for your setup
    // If you're using a framework that allows capturing log messages in tests, prepare it here

    // Execute the function under test
    val result = FlintREPL.canPickNextStatement(sessionId, jobId, osClient, sessionIndex)

    // Assert the function returns true
    assert(result)
  }

  test("test canPickNextStatement: OSClient Throws Exception") {
    val sessionId = "session123"
    val jobId = "jobABC"
    val osClient = mock[OSClient]
    val sessionIndex = "sessionIndex"

    // Set up the mock OSClient to throw an exception
    when(osClient.getDoc(sessionIndex, sessionId))
      .thenThrow(new RuntimeException("OpenSearch cluster unresponsive"))

    // Mock or redirect the logger if necessary
    // For some logging frameworks, you might be able to use a TestAppender or similar mechanism

    // Execute the method under test and expect true, since the method is designed to return true even in case of an exception
    val result = FlintREPL.canPickNextStatement(sessionId, jobId, osClient, sessionIndex)

    // Verify the result is true despite the exception
    assert(result)
  }

  test(
    "test canPickNextStatement: Doc Exists and excludeJobIds is a Single String Not Matching JobId") {
    val sessionId = "session123"
    val jobId = "jobABC"
    val nonMatchingExcludeJobId = "jobXYZ" // This ID does not match the jobId
    val osClient = mock[OSClient]
    val sessionIndex = "sessionIndex"

    val getResponse = mock[GetResponse]
    when(osClient.getDoc(sessionIndex, sessionId)).thenReturn(getResponse)
    when(getResponse.isExists()).thenReturn(true)

    // Create a sourceMap with excludeJobIds as a String that does NOT match jobId
    val sourceMap: java.util.Map[String, Object] = new java.util.HashMap[String, Object]()
    sourceMap.put("jobId", jobId.asInstanceOf[Object])
    sourceMap.put("excludeJobIds", nonMatchingExcludeJobId.asInstanceOf[Object])

    when(getResponse.getSourceAsMap).thenReturn(sourceMap)

    // Execute the method under test
    val result = FlintREPL.canPickNextStatement(sessionId, jobId, osClient, sessionIndex)

    // The function should return true since jobId is not excluded
    assert(result)
  }

  test("Doc Exists and excludeJobIds is an ArrayList Containing JobId") {
    val sessionId = "session123"
    val jobId = "jobABC"
    val osClient = mock[OSClient]
    val sessionIndex = "sessionIndex"
    val handleSessionError = mock[Function1[String, Unit]]

    val getResponse = mock[GetResponse]
    when(osClient.getDoc(sessionIndex, sessionId)).thenReturn(getResponse)
    when(getResponse.isExists()).thenReturn(true)

    // Create a sourceMap with excludeJobIds as an ArrayList containing jobId
    val sourceMap: java.util.Map[String, Object] = new java.util.HashMap[String, Object]()
    sourceMap.put("jobId", jobId.asInstanceOf[Object])

    // Creating an ArrayList and adding the jobId to it
    val excludeJobIdsList = new java.util.ArrayList[String]()
    excludeJobIdsList.add(jobId)
    sourceMap.put("excludeJobIds", excludeJobIdsList.asInstanceOf[Object])

    when(getResponse.getSourceAsMap).thenReturn(sourceMap)

    // Execute the method under test
    val result = FlintREPL.canPickNextStatement(sessionId, jobId, osClient, sessionIndex)

    // The function should return false since jobId is excluded
    assert(!result)
  }

  test("Doc Exists and excludeJobIds is an ArrayList Not Containing JobId") {
    val sessionId = "session123"
    val jobId = "jobABC"
    val osClient = mock[OSClient]
    val sessionIndex = "sessionIndex"

    val getResponse = mock[GetResponse]
    when(osClient.getDoc(sessionIndex, sessionId)).thenReturn(getResponse)
    when(getResponse.isExists()).thenReturn(true)

    // Create a sourceMap with excludeJobIds as an ArrayList not containing jobId
    val sourceMap: java.util.Map[String, Object] = new java.util.HashMap[String, Object]()
    sourceMap.put("jobId", jobId.asInstanceOf[Object])

    // Creating an ArrayList and adding a different jobId to it
    val excludeJobIdsList = new java.util.ArrayList[String]()
    excludeJobIdsList.add("jobXYZ") // This ID does not match the jobId
    sourceMap.put("excludeJobIds", excludeJobIdsList.asInstanceOf[Object])

    when(getResponse.getSourceAsMap).thenReturn(sourceMap)

    // Execute the method under test
    val result = FlintREPL.canPickNextStatement(sessionId, jobId, osClient, sessionIndex)

    // The function should return true since the jobId is not in the excludeJobIds list
    assert(result)
  }

  test("exponentialBackoffRetry should retry on ConnectException") {
    val mockReader = mock[OpenSearchReader]
    val exception = new RuntimeException(
      new ConnectException(
        "Timeout connecting to [search-foo-1-bar.eu-west-1.es.amazonaws.com:443]"))
    val osClient = mock[OSClient]
    when(osClient.createReader(any[String], any[String], any[String])).thenReturn(mockReader)
    when(mockReader.hasNext).thenThrow(exception)

    val maxRetries = 1
    var actualRetries = 0

    val resultIndex = "testResultIndex"
    val dataSource = "testDataSource"
    val sessionIndex = "testSessionIndex"
    val sessionId = "testSessionId"
    val jobId = "testJobId"
    val applicationId = "testApplicationId"

    // Create a SparkSession for testing (use master("local") for local testing)
    try {
      val spark = SparkSession.builder().master("local").appName("FlintREPLTest").getOrCreate()

      val flintSessionIndexUpdater = mock[OpenSearchUpdater]

      intercept[RuntimeException] {
        FlintREPL.exponentialBackoffRetry(maxRetries, 2.seconds) {
          actualRetries += 1
          FlintREPL.queryLoop(
            resultIndex,
            dataSource,
            sessionIndex,
            sessionId,
            spark,
            osClient,
            jobId,
            applicationId,
            flintSessionIndexUpdater)
        }
      } // (block)

      assert(actualRetries == maxRetries)
    } finally {
      // Stop the SparkSession
      spark.stop()
    }
  }

  test("executeAndHandle should handle TimeoutException properly") {
    val mockSparkSession: SparkSession = mock[SparkSession]
    val mockFlintCommand: FlintCommand = mock[FlintCommand]
    // val mockExecutionContextExecutor: ExecutionContextExecutor = mock[ExecutionContextExecutor]
    val threadPool = ThreadUtils.newDaemonThreadPoolScheduledExecutor("flint-repl", 1)
    implicit val executionContext = ExecutionContext.fromExecutor(threadPool)

    try {
      val dataSource = "someDataSource"
      val sessionId = "someSessionId"
      val startTime = System.currentTimeMillis()
      val expectedDataFrame = mock[DataFrame]

      when(mockFlintCommand.query).thenReturn("SELECT 1")
      when(mockFlintCommand.submitTime).thenReturn(Instant.now().toEpochMilli())
      // When the `sql` method is called, execute the custom Answer that introduces a delay
      when(mockSparkSession.sql(any[String])).thenAnswer(new Answer[DataFrame] {
        override def answer(invocation: InvocationOnMock): DataFrame = {
          // Introduce a delay of 60 seconds
          Thread.sleep(60000)

          expectedDataFrame
        }
      })

      when(mockSparkSession.createDataFrame(any[Seq[Product]])(any[TypeTag[Product]]))
        .thenReturn(expectedDataFrame)

      when(expectedDataFrame.toDF(any[Seq[String]]: _*)).thenReturn(expectedDataFrame)

      val sparkContext: SparkContext = mock[SparkContext]
      when(mockSparkSession.sparkContext).thenReturn(sparkContext)

      val result = FlintREPL.executeAndHandle(
        mockSparkSession,
        mockFlintCommand,
        dataSource,
        sessionId,
        executionContext,
        startTime,
        // make sure it times out before mockSparkSession.sql can return, which takes 60 seconds
        Duration(1, SECONDS))

      verify(mockSparkSession, times(1)).sql(any[String])
      verify(sparkContext, times(1)).cancelJobGroup(any[String])
      result should not be None
    } finally {
      threadPool.shutdown()
    }
  }

  test("executeAndHandle should handle ParseException properly") {
    val mockSparkSession: SparkSession = mock[SparkSession]
    val flintCommand: FlintCommand =
      new FlintCommand(
        "Running",
        "select * from default.http_logs limit1 1",
        "10",
        "20",
        Instant.now().toEpochMilli,
        None)
    val threadPool = ThreadUtils.newDaemonThreadPoolScheduledExecutor("flint-repl", 1)
    implicit val executionContext = ExecutionContext.fromExecutor(threadPool)

    try {
      val dataSource = "someDataSource"
      val sessionId = "someSessionId"
      val startTime = System.currentTimeMillis()
      val expectedDataFrame = mock[DataFrame]

      // sql method can only throw RuntimeException
      when(mockSparkSession.sql(any[String])).thenThrow(
        new RuntimeException(new ParseException(None, "INVALID QUERY", Origin(), Origin())))
      val sparkContext: SparkContext = mock[SparkContext]
      when(mockSparkSession.sparkContext).thenReturn(sparkContext)

      // Assume handleQueryException logs the error and returns an error message string
      val mockErrorString = "Error due to syntax"
      when(mockSparkSession.createDataFrame(any[Seq[Product]])(any[TypeTag[Product]]))
        .thenReturn(expectedDataFrame)
      when(expectedDataFrame.toDF(any[Seq[String]]: _*)).thenReturn(expectedDataFrame)

      val result = FlintREPL.executeAndHandle(
        mockSparkSession,
        flintCommand,
        dataSource,
        sessionId,
        executionContext,
        startTime,
        Duration.Inf // Use Duration.Inf or a large enough duration to avoid a timeout
      )

      // Verify that ParseException was caught and handled
      result should not be None // or result.isDefined shouldBe true
      flintCommand.error should not be None
      flintCommand.error.get should include("Syntax error:")
    } finally {
      threadPool.shutdown()
    }

  }

}
