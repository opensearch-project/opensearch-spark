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
import scala.concurrent.duration.{Duration, MINUTES}
import scala.reflect.runtime.universe.TypeTag

import com.codahale.metrics.Timer
import org.mockito.ArgumentMatchers.{eq => eqTo, _}
import org.mockito.ArgumentMatchersSugar
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.opensearch.action.get.GetResponse
import org.opensearch.flint.app.FlintCommand
import org.opensearch.flint.core.storage.{FlintReader, OpenSearchReader, OpenSearchUpdater}
import org.opensearch.search.sort.SortOrder
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.{SparkConf, SparkContext, SparkFunSuite}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.trees.Origin
import org.apache.spark.sql.flint.config.FlintSparkConf
import org.apache.spark.sql.types.{LongType, NullType, StringType, StructField, StructType}
import org.apache.spark.sql.util.{DefaultThreadPoolFactory, MockThreadPoolFactory, MockTimeProvider, RealTimeProvider, ShutdownHookManagerTrait}
import org.apache.spark.util.ThreadUtils

class FlintREPLTest
    extends SparkFunSuite
    with MockitoSugar
    with ArgumentMatchersSugar
    with JobMatchers {
  // By using a type alias and casting, I can bypass the type checking error.
  type AnyScheduledFuture = ScheduledFuture[_]

  test(
    "parseArgs with one argument should return None for query and the argument as resultIndex") {
    val args = Array("resultIndexName")
    val (queryOption, resultIndex) = FlintREPL.parseArgs(args)
    queryOption shouldBe None
    resultIndex shouldBe "resultIndexName"
  }

  test(
    "parseArgs with two arguments should return the first argument as query and the second as resultIndex") {
    val args = Array("SELECT * FROM table", "resultIndexName")
    val (queryOption, resultIndex) = FlintREPL.parseArgs(args)
    queryOption shouldBe Some("SELECT * FROM table")
    resultIndex shouldBe "resultIndexName"
  }

  test(
    "parseArgs with no arguments should throw IllegalArgumentException with specific message") {
    val args = Array.empty[String]
    val exception = intercept[IllegalArgumentException] {
      FlintREPL.parseArgs(args)
    }
    exception.getMessage shouldBe "Unsupported number of arguments. Expected 1 or 2 arguments."
  }

  test(
    "parseArgs with more than two arguments should throw IllegalArgumentException with specific message") {
    val args = Array("arg1", "arg2", "arg3")
    val exception = intercept[IllegalArgumentException] {
      FlintREPL.parseArgs(args)
    }
    exception.getMessage shouldBe "Unsupported number of arguments. Expected 1 or 2 arguments."
  }

  test("getQuery should return query from queryOption if present") {
    val queryOption = Some("SELECT * FROM table")
    val jobType = "streaming"
    val conf = new SparkConf()

    val query = FlintREPL.getQuery(queryOption, jobType, conf)
    query shouldBe "SELECT * FROM table"
  }

  test("getQuery should return default query for streaming job if queryOption is None") {
    val queryOption = None
    val jobType = "streaming"
    val conf = new SparkConf().set(FlintSparkConf.QUERY.key, "SELECT * FROM table")

    val query = FlintREPL.getQuery(queryOption, jobType, conf)
    query shouldBe "SELECT * FROM table"
  }

  test(
    "getQuery should throw IllegalArgumentException if queryOption is None and default query is not defined for streaming job") {
    val queryOption = None
    val jobType = "streaming"
    val conf = new SparkConf() // Default query not set

    intercept[IllegalArgumentException] {
      FlintREPL.getQuery(queryOption, jobType, conf)
    }.getMessage shouldBe "Query undefined for the streaming job."
  }

  test("getQuery should return empty string for non-streaming job if queryOption is None") {
    val queryOption = None
    val jobType = "interactive"
    val conf = new SparkConf() // Default query not needed

    val query = FlintREPL.getQuery(queryOption, jobType, conf)
    query shouldBe ""
  }

  test("createHeartBeatUpdater should update heartbeat correctly") {
    // Mocks
    val flintSessionUpdater = mock[OpenSearchUpdater]
    val osClient = mock[OSClient]
    val threadPool = mock[ScheduledExecutorService]
    val getResponse = mock[GetResponse]
    val scheduledFutureRaw = mock[ScheduledFuture[_]]

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
      "sessionIndex",
      0)

    // Verifications
    verify(flintSessionUpdater, atLeastOnce()).upsert(eqTo("session1"), *)
  }

  test("createShutdownHook add shutdown hook and update FlintInstance if conditions are met") {
    val flintSessionIndexUpdater = mock[OpenSearchUpdater]
    val osClient = mock[OSClient]
    val getResponse = mock[GetResponse]
    val sessionIndex = "testIndex"
    val sessionId = "testSessionId"
    val flintSessionContext = mock[Timer.Context]

    when(osClient.getDoc(sessionIndex, sessionId)).thenReturn(getResponse)
    when(getResponse.isExists()).thenReturn(true)

    when(getResponse.getSourceAsMap).thenReturn(
      Map[String, Object](
        "applicationId" -> "app1",
        "jobId" -> "job1",
        "sessionId" -> "session1",
        "state" -> "running",
        "lastUpdateTime" -> java.lang.Long.valueOf(12345L),
        "error" -> "someError",
        "state" -> "running",
        "jobStartTime" -> java.lang.Long.valueOf(0L)).asJava)

    val mockShutdownHookManager = new ShutdownHookManagerTrait {
      override def addShutdownHook(hook: () => Unit): AnyRef = {
        hook() // execute the hook immediately
        new Object() // return a dummy AnyRef as per the method signature
      }
    }

    // Here, we're injecting our mockShutdownHookManager into the method
    FlintREPL.addShutdownHook(
      flintSessionIndexUpdater,
      osClient,
      sessionIndex,
      sessionId,
      flintSessionContext,
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

    val currentTime = System.currentTimeMillis()
    val queryRunTime = 3000L

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
    val spark = SparkSession.builder().appName("Test").master("local").getOrCreate()

    val expected =
      spark.createDataFrame(spark.sparkContext.parallelize(expectedRows), expectedSchema)

    val flintCommand = new FlintCommand("failed", "select 1", "30", "10", currentTime, None)

    try {
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
    } finally {
      spark.close()
      FlintREPL.currentTimeProvider = new RealTimeProvider()

    }
  }

  test("test canPickNextStatement: Doc Exists and Valid JobId") {
    val sessionId = "session123"
    val jobId = "jobABC"
    val osClient = mock[OSClient]
    val sessionIndex = "sessionIndex"

    val getResponse = mock[GetResponse]
    when(osClient.getDoc(sessionIndex, sessionId)).thenReturn(getResponse)
    when(getResponse.isExists()).thenReturn(true)

    val sourceMap = new java.util.HashMap[String, Object]()
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

    val sourceMap = new java.util.HashMap[String, Object]()
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

    val sourceMap = new java.util.HashMap[String, Object]()
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

    val sourceMap = new java.util.HashMap[String, Object]()
    sourceMap.put("jobId", jobId)
    sourceMap.put(
      "excludeJobIds",
      Integer.valueOf(123)
    ) // Using an Integer here to represent an unexpected type

    when(getResponse.getSourceAsMap).thenReturn(sourceMap)

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
    val sourceMap = new java.util.HashMap[String, Object]()
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
    val sourceMap = new java.util.HashMap[String, Object]()
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
    val sourceMap = new java.util.HashMap[String, Object]()
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
    when(osClient.createQueryReader(any[String], any[String], any[String], eqTo(SortOrder.ASC)))
      .thenReturn(mockReader)
    when(mockReader.hasNext).thenThrow(exception)

    val maxRetries = 1
    var actualRetries = 0

    val resultIndex = "testResultIndex"
    val dataSource = "testDataSource"
    val sessionIndex = "testSessionIndex"
    val sessionId = "testSessionId"
    val jobId = "testJobId"
    val applicationId = "testApplicationId"

    val spark = SparkSession.builder().master("local").appName("FlintREPLTest").getOrCreate()
    try {
      val flintSessionIndexUpdater = mock[OpenSearchUpdater]

      val commandContext = CommandContext(
        spark,
        dataSource,
        resultIndex,
        sessionId,
        flintSessionIndexUpdater,
        osClient,
        sessionIndex,
        jobId,
        Duration(10, MINUTES),
        60,
        60)

      intercept[RuntimeException] {
        FlintREPL.exponentialBackoffRetry(maxRetries, 2.seconds) {
          actualRetries += 1
          FlintREPL.queryLoop(commandContext)
        }
      }

      assert(actualRetries == maxRetries)
    } finally {
      // Stop the SparkSession
      spark.stop()
    }
  }

  test("executeAndHandle should handle TimeoutException properly") {
    val mockSparkSession = mock[SparkSession]
    val mockFlintCommand = mock[FlintCommand]
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

      val sparkContext = mock[SparkContext]
      when(mockSparkSession.sparkContext).thenReturn(sparkContext)

      val result = FlintREPL.executeAndHandle(
        mockSparkSession,
        mockFlintCommand,
        dataSource,
        sessionId,
        executionContext,
        startTime,
        // make sure it times out before mockSparkSession.sql can return, which takes 60 seconds
        Duration(1, SECONDS),
        600000)

      verify(mockSparkSession, times(1)).sql(any[String])
      verify(sparkContext, times(1)).cancelJobGroup(any[String])
      result should not be None
    } finally threadPool.shutdown()
  }

  test("executeAndHandle should handle ParseException properly") {
    val mockSparkSession = mock[SparkSession]
    val flintCommand =
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
      val sparkContext = mock[SparkContext]
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
        Duration.Inf, // Use Duration.Inf or a large enough duration to avoid a timeout,
        600000)

      // Verify that ParseException was caught and handled
      result should not be None // or result.isDefined shouldBe true
      flintCommand.error should not be None
      flintCommand.error.get should include("Syntax error:")
    } finally threadPool.shutdown()

  }

  test("setupFlintJobWithExclusionCheck should proceed normally when no jobs are excluded") {
    val osClient = mock[OSClient]
    val getResponse = mock[GetResponse]
    when(osClient.getDoc(*, *)).thenReturn(getResponse)
    when(getResponse.isExists()).thenReturn(true)
    when(getResponse.getSourceAsMap).thenReturn(
      Map[String, Object](
        "applicationId" -> "app1",
        "jobId" -> "job1",
        "sessionId" -> "session1",
        "lastUpdateTime" -> java.lang.Long.valueOf(12345L),
        "error" -> "someError",
        "state" -> "running",
        "jobStartTime" -> java.lang.Long.valueOf(0L)).asJava)
    when(getResponse.getSeqNo).thenReturn(0L)
    when(getResponse.getPrimaryTerm).thenReturn(0L)

    val flintSessionIndexUpdater = mock[OpenSearchUpdater]
    val mockConf = new SparkConf().set("spark.flint.deployment.excludeJobs", "")

    // other mock objects like osClient, flintSessionIndexUpdater with necessary mocking
    val result = FlintREPL.setupFlintJobWithExclusionCheck(
      mockConf,
      Some("sessionIndex"),
      Some("sessionId"),
      osClient,
      "jobId",
      "appId",
      flintSessionIndexUpdater,
      System.currentTimeMillis())
    assert(!result) // Expecting false as the job should proceed normally
  }

  test("setupFlintJobWithExclusionCheck should exit early if current job is excluded") {
    val osClient = mock[OSClient]
    val getResponse = mock[GetResponse]
    when(osClient.getDoc(*, *)).thenReturn(getResponse)
    when(getResponse.isExists()).thenReturn(true)
    // Mock the rest of the GetResponse as needed

    val flintSessionIndexUpdater = mock[OpenSearchUpdater]
    val mockConf = new SparkConf().set("spark.flint.deployment.excludeJobs", "jobId")

    val result = FlintREPL.setupFlintJobWithExclusionCheck(
      mockConf,
      Some("sessionIndex"),
      Some("sessionId"),
      osClient,
      "jobId",
      "appId",
      flintSessionIndexUpdater,
      System.currentTimeMillis())
    assert(result) // Expecting true as the job should exit early
  }

  test("setupFlintJobWithExclusionCheck should exit early if a duplicate job is running") {
    val osClient = mock[OSClient]
    val getResponse = mock[GetResponse]
    when(osClient.getDoc(*, *)).thenReturn(getResponse)
    when(getResponse.isExists()).thenReturn(true)
    // Mock the GetResponse to simulate a scenario of a duplicate job
    when(getResponse.getSourceAsMap).thenReturn(
      Map[String, Object](
        "applicationId" -> "app1",
        "jobId" -> "job1",
        "sessionId" -> "session1",
        "lastUpdateTime" -> java.lang.Long.valueOf(12345L),
        "error" -> "someError",
        "state" -> "running",
        "jobStartTime" -> java.lang.Long.valueOf(0L),
        "excludeJobIds" -> java.util.Arrays
          .asList("job-2", "job-1") // Include this inside the Map
      ).asJava)

    val flintSessionIndexUpdater = mock[OpenSearchUpdater]
    val mockConf = new SparkConf().set("spark.flint.deployment.excludeJobs", "job-1,job-2")

    val result = FlintREPL.setupFlintJobWithExclusionCheck(
      mockConf,
      Some("sessionIndex"),
      Some("sessionId"),
      osClient,
      "jobId",
      "appId",
      flintSessionIndexUpdater,
      System.currentTimeMillis())
    assert(result) // Expecting true for early exit due to duplicate job
  }

  test("setupFlintJobWithExclusionCheck should setup job normally when conditions are met") {
    val osClient = mock[OSClient]
    val getResponse = mock[GetResponse]
    when(osClient.getDoc(*, *)).thenReturn(getResponse)
    when(getResponse.isExists()).thenReturn(true)

    val flintSessionIndexUpdater = mock[OpenSearchUpdater]
    val mockConf = new SparkConf().set("spark.flint.deployment.excludeJobs", "job-3,job-4")

    val result = FlintREPL.setupFlintJobWithExclusionCheck(
      mockConf,
      Some("sessionIndex"),
      Some("sessionId"),
      osClient,
      "jobId",
      "appId",
      flintSessionIndexUpdater,
      System.currentTimeMillis())
    assert(!result) // Expecting false as the job proceeds normally
  }

  test(
    "setupFlintJobWithExclusionCheck should throw NoSuchElementException if sessionIndex or sessionId is missing") {
    val osClient = mock[OSClient]
    val flintSessionIndexUpdater = mock[OpenSearchUpdater]
    val mockConf = new SparkConf().set("spark.flint.deployment.excludeJobs", "")

    assertThrows[NoSuchElementException] {
      FlintREPL.setupFlintJobWithExclusionCheck(
        mockConf,
        None, // No sessionIndex provided
        None, // No sessionId provided
        osClient,
        "jobId",
        "appId",
        flintSessionIndexUpdater,
        System.currentTimeMillis())
    }
  }

  test("queryLoop continue until inactivity limit is reached") {
    val mockReader = mock[FlintReader]
    val osClient = mock[OSClient]
    when(osClient.createQueryReader(any[String], any[String], any[String], eqTo(SortOrder.ASC)))
      .thenReturn(mockReader)
    when(mockReader.hasNext).thenReturn(false)

    val resultIndex = "testResultIndex"
    val dataSource = "testDataSource"
    val sessionIndex = "testSessionIndex"
    val sessionId = "testSessionId"
    val jobId = "testJobId"

    val shortInactivityLimit = 500 // 500 milliseconds

    // Create a SparkSession for testing
    val spark = SparkSession.builder().master("local").appName("FlintREPLTest").getOrCreate()

    val flintSessionIndexUpdater = mock[OpenSearchUpdater]

    val commandContext = CommandContext(
      spark,
      dataSource,
      resultIndex,
      sessionId,
      flintSessionIndexUpdater,
      osClient,
      sessionIndex,
      jobId,
      Duration(10, MINUTES),
      shortInactivityLimit,
      60)

    // Mock processCommands to always allow loop continuation
    val getResponse = mock[GetResponse]
    when(osClient.getDoc(*, *)).thenReturn(getResponse)
    when(getResponse.isExists()).thenReturn(false)

    val startTime = System.currentTimeMillis()

    FlintREPL.queryLoop(commandContext)

    val endTime = System.currentTimeMillis()

    // Check if the loop ran for approximately the duration of the inactivity limit
    assert(endTime - startTime >= shortInactivityLimit)

    // Stop the SparkSession
    spark.stop()
  }

  test("queryLoop should stop when canPickUpNextStatement is false") {
    val mockReader = mock[FlintReader]
    val osClient = mock[OSClient]
    when(osClient.createQueryReader(any[String], any[String], any[String], eqTo(SortOrder.ASC)))
      .thenReturn(mockReader)
    when(mockReader.hasNext).thenReturn(true)

    val resultIndex = "testResultIndex"
    val dataSource = "testDataSource"
    val sessionIndex = "testSessionIndex"
    val sessionId = "testSessionId"
    val jobId = "testJobId"
    val longInactivityLimit = 10000 // 10 seconds

    // Create a SparkSession for testing
    val spark = SparkSession.builder().master("local").appName("FlintREPLTest").getOrCreate()

    val flintSessionIndexUpdater = mock[OpenSearchUpdater]

    val commandContext = CommandContext(
      spark,
      dataSource,
      resultIndex,
      sessionId,
      flintSessionIndexUpdater,
      osClient,
      sessionIndex,
      jobId,
      Duration(10, MINUTES),
      longInactivityLimit,
      60)

    // Mocking canPickNextStatement to return false
    when(osClient.getDoc(sessionIndex, sessionId)).thenAnswer(_ => {
      val mockGetResponse = mock[GetResponse]
      when(mockGetResponse.isExists()).thenReturn(true)
      val sourceMap = new java.util.HashMap[String, Object]()
      sourceMap.put("jobId", "differentJobId")
      when(mockGetResponse.getSourceAsMap).thenReturn(sourceMap)
      mockGetResponse
    })

    val startTime = System.currentTimeMillis()

    FlintREPL.queryLoop(commandContext)

    val endTime = System.currentTimeMillis()

    // Check if the loop stopped before the inactivity limit
    assert(endTime - startTime < longInactivityLimit)

    // Stop the SparkSession
    spark.stop()
  }

  test("queryLoop should properly shut down the thread pool after execution") {
    val mockReader = mock[FlintReader]
    val osClient = mock[OSClient]
    when(osClient.createQueryReader(any[String], any[String], any[String], eqTo(SortOrder.ASC)))
      .thenReturn(mockReader)
    when(mockReader.hasNext).thenReturn(false)

    val resultIndex = "testResultIndex"
    val dataSource = "testDataSource"
    val sessionIndex = "testSessionIndex"
    val sessionId = "testSessionId"
    val jobId = "testJobId"

    val inactivityLimit = 500 // 500 milliseconds

    // Create a SparkSession for testing
    val spark = SparkSession.builder().master("local").appName("FlintREPLTest").getOrCreate()

    val flintSessionIndexUpdater = mock[OpenSearchUpdater]

    val commandContext = CommandContext(
      spark,
      dataSource,
      resultIndex,
      sessionId,
      flintSessionIndexUpdater,
      osClient,
      sessionIndex,
      jobId,
      Duration(10, MINUTES),
      inactivityLimit,
      60)

    try {
      // Mocking ThreadUtils to track the shutdown call
      val mockThreadPool = mock[ScheduledExecutorService]
      FlintREPL.threadPoolFactory = new MockThreadPoolFactory(mockThreadPool)

      FlintREPL.queryLoop(commandContext)

      // Verify if the shutdown method was called on the thread pool
      verify(mockThreadPool).shutdown()
    } finally {
      // Stop the SparkSession
      spark.stop()
      FlintREPL.threadPoolFactory = new DefaultThreadPoolFactory()
    }
  }

  test("queryLoop handle exceptions within the loop gracefully") {
    val mockReader = mock[FlintReader]
    val osClient = mock[OSClient]
    when(osClient.createQueryReader(any[String], any[String], any[String], eqTo(SortOrder.ASC)))
      .thenReturn(mockReader)
    // Simulate an exception thrown when hasNext is called
    when(mockReader.hasNext).thenThrow(new RuntimeException("Test exception"))

    val resultIndex = "testResultIndex"
    val dataSource = "testDataSource"
    val sessionIndex = "testSessionIndex"
    val sessionId = "testSessionId"
    val jobId = "testJobId"

    val inactivityLimit = 500 // 500 milliseconds

    // Create a SparkSession for testing
    val spark = SparkSession.builder().master("local").appName("FlintREPLTest").getOrCreate()

    val flintSessionIndexUpdater = mock[OpenSearchUpdater]

    val commandContext = CommandContext(
      spark,
      dataSource,
      resultIndex,
      sessionId,
      flintSessionIndexUpdater,
      osClient,
      sessionIndex,
      jobId,
      Duration(10, MINUTES),
      inactivityLimit,
      60)

    try {
      // Mocking ThreadUtils to track the shutdown call
      val mockThreadPool = mock[ScheduledExecutorService]
      FlintREPL.threadPoolFactory = new MockThreadPoolFactory(mockThreadPool)

      intercept[RuntimeException] {
        FlintREPL.queryLoop(commandContext)
      }

      // Verify if the shutdown method was called on the thread pool
      verify(mockThreadPool).shutdown()
    } finally {
      // Stop the SparkSession
      spark.stop()
      FlintREPL.threadPoolFactory = new DefaultThreadPoolFactory()
    }
  }

  test("queryLoop should correctly update loop control variables") {
    val mockReader = mock[FlintReader]
    val osClient = mock[OSClient]
    when(osClient.createQueryReader(any[String], any[String], any[String], eqTo(SortOrder.ASC)))
      .thenReturn(mockReader)
    val getResponse = mock[GetResponse]
    when(osClient.getDoc(*, *)).thenReturn(getResponse)
    when(getResponse.isExists()).thenReturn(false)
    when(osClient.doesIndexExist(*)).thenReturn(true)
    when(osClient.getIndexMetadata(*)).thenReturn(FlintREPL.resultIndexMapping)

    // Configure mockReader to return true once and then false to exit the loop
    when(mockReader.hasNext).thenReturn(true).thenReturn(false)
    val command =
      """ {
          "state": "running",
          "query": "SELECT * FROM table",
          "statementId": "stmt123",
          "queryId": "query456",
          "submitTime": 1234567890,
          "error": "Some error"
        }
        """
    when(mockReader.next).thenReturn(command)

    val resultIndex = "testResultIndex"
    val dataSource = "testDataSource"
    val sessionIndex = "testSessionIndex"
    val sessionId = "testSessionId"
    val jobId = "testJobId"

    val inactivityLimit = 5000 // 5 seconds

    // Create a SparkSession for testing\
    val mockSparkSession = mock[SparkSession]
    val expectedDataFrame = mock[DataFrame]
    when(mockSparkSession.createDataFrame(any[Seq[Product]])(any[TypeTag[Product]]))
      .thenReturn(expectedDataFrame)
    val sparkContext = mock[SparkContext]
    when(mockSparkSession.sparkContext).thenReturn(sparkContext)

    when(expectedDataFrame.toDF(any[Seq[String]]: _*)).thenReturn(expectedDataFrame)

    val flintSessionIndexUpdater = mock[OpenSearchUpdater]

    val commandContext = CommandContext(
      mockSparkSession,
      dataSource,
      resultIndex,
      sessionId,
      flintSessionIndexUpdater,
      osClient,
      sessionIndex,
      jobId,
      Duration(10, MINUTES),
      inactivityLimit,
      60)

    val startTime = Instant.now().toEpochMilli()

    // Running the queryLoop
    FlintREPL.queryLoop(commandContext)

    val endTime = Instant.now().toEpochMilli()

    // Assuming processCommands updates the lastActivityTime to the current time
    assert(endTime - startTime >= inactivityLimit)
    verify(osClient, times(1)).getIndexMetadata(*)
  }

  test("queryLoop should execute loop without processing any commands") {
    val mockReader = mock[FlintReader]
    val osClient = mock[OSClient]
    when(osClient.createQueryReader(any[String], any[String], any[String], eqTo(SortOrder.ASC)))
      .thenReturn(mockReader)
    val getResponse = mock[GetResponse]
    when(osClient.getDoc(*, *)).thenReturn(getResponse)
    when(getResponse.isExists()).thenReturn(false)

    // Configure mockReader to always return false, indicating no commands to process
    when(mockReader.hasNext).thenReturn(false)

    val resultIndex = "testResultIndex"
    val dataSource = "testDataSource"
    val sessionIndex = "testSessionIndex"
    val sessionId = "testSessionId"
    val jobId = "testJobId"

    val inactivityLimit = 5000 // 5 seconds

    // Create a SparkSession for testing
    val spark = SparkSession.builder().master("local").appName("FlintREPLTest").getOrCreate()

    val flintSessionIndexUpdater = mock[OpenSearchUpdater]

    val commandContext = CommandContext(
      spark,
      dataSource,
      resultIndex,
      sessionId,
      flintSessionIndexUpdater,
      osClient,
      sessionIndex,
      jobId,
      Duration(10, MINUTES),
      inactivityLimit,
      60)

    val startTime = Instant.now().toEpochMilli()

    // Running the queryLoop
    FlintREPL.queryLoop(commandContext)

    val endTime = Instant.now().toEpochMilli()

    // Assert that the loop ran for at least the duration of the inactivity limit
    assert(endTime - startTime >= inactivityLimit)

    // Verify that no command was actually processed
    verify(mockReader, never()).next()

    // Stop the SparkSession
    spark.stop()
  }
}
