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

import com.amazonaws.services.glue.model.AccessDeniedException
import com.codahale.metrics.Timer
import org.mockito.{ArgumentMatchersSugar, Mockito}
import org.mockito.Mockito.{atLeastOnce, doNothing, never, times, verify, when}
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.opensearch.action.get.GetResponse
import org.opensearch.flint.common.model.{FlintStatement, InteractiveSession, SessionStates}
import org.opensearch.flint.common.scheduler.model.LangType
import org.opensearch.flint.core.storage.{FlintReader, OpenSearchReader, OpenSearchUpdater}
import org.opensearch.search.sort.SortOrder
import org.scalatest.prop.TableDrivenPropertyChecks._
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.{SparkConf, SparkContext, SparkFunSuite}
import org.apache.spark.scheduler.SparkListenerApplicationEnd
import org.apache.spark.sql.FlintREPL.PreShutdownListener
import org.apache.spark.sql.FlintREPLConfConstants.DEFAULT_QUERY_LOOP_EXECUTION_FREQUENCY
import org.apache.spark.sql.SparkConfConstants.{DEFAULT_SQL_EXTENSIONS, SQL_EXTENSIONS_KEY}
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

  private val jobId = "testJobId"
  private val applicationId = "testApplicationId"
  private val INTERACTIVE_JOB_TYPE = "interactive"

  test("parseArgs with no arguments should return (None, None)") {
    val args = Array.empty[String]
    val (queryOption, resultIndexOption) = FlintREPL.parseArgs(args)
    queryOption shouldBe None
    resultIndexOption shouldBe None
  }

  test("parseArgs with one argument should return None for query and Some for resultIndex") {
    val args = Array("resultIndexName")
    val (queryOption, resultIndexOption) = FlintREPL.parseArgs(args)
    queryOption shouldBe None
    resultIndexOption shouldBe Some("resultIndexName")
  }

  test("parseArgs with two arguments should return Some for both query and resultIndex") {
    val args = Array("SELECT * FROM table", "resultIndexName")
    val (queryOption, resultIndexOption) = FlintREPL.parseArgs(args)
    queryOption shouldBe Some("SELECT * FROM table")
    resultIndexOption shouldBe Some("resultIndexName")
  }

  test(
    "parseArgs with more than two arguments should throw IllegalArgumentException with specific message") {
    val args = Array("arg1", "arg2", "arg3")
    val exception = intercept[IllegalArgumentException] {
      FlintREPL.parseArgs(args)
    }
    exception.getMessage shouldBe "Unsupported number of arguments. Expected no more than two arguments."
  }

  test("getSessionId should throw exception when SESSION_ID is not set") {
    val conf = new SparkConf()
    val exception = intercept[IllegalArgumentException] {
      FlintREPL.getSessionId(conf)
    }
    assert(exception.getMessage === FlintSparkConf.SESSION_ID.key + " is not set or is empty")
  }

  test("getSessionId should return the session ID when it's set") {
    val sessionId = "test-session-id"
    val conf = new SparkConf().set(FlintSparkConf.SESSION_ID.key, sessionId)
    assert(FlintREPL.getSessionId(conf) === sessionId)
  }

  test("getSessionId should throw exception when SESSION_ID is set to empty string") {
    val conf = new SparkConf().set(FlintSparkConf.SESSION_ID.key, "")
    val exception = intercept[IllegalArgumentException] {
      FlintREPL.getSessionId(conf)
    }
    assert(exception.getMessage === FlintSparkConf.SESSION_ID.key + " is not set or is empty")
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
    "getQuery should return unescaped default query for streaming job if queryOption is None") {
    val queryOption = None
    val jobType = "streaming"
    val conf = new SparkConf().set(
      FlintSparkConf.QUERY.key,
      "SELECT \\\"1\\\" UNION SELECT '\\\"1\\\"' UNION SELECT \\\"\\\\\\\"1\\\\\\\"\\\"")

    val query = FlintREPL.getQuery(queryOption, jobType, conf)
    query shouldBe "SELECT \"1\" UNION SELECT '\"1\"' UNION SELECT \"\\\"1\\\"\""
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

  test("createSparkConf should set the app name and default SQL extensions") {
    val conf = FlintREPL.createSparkConf()

    // Assert that the app name is set correctly
    assert(conf.get("spark.app.name") === "FlintREPL$")

    // Assert that the default SQL extensions are set correctly
    assert(conf.get(SQL_EXTENSIONS_KEY) === DEFAULT_SQL_EXTENSIONS)
  }

  test(
    "createSparkConf should not use defaultExtensions if spark.sql.extensions is already set") {
    val customExtension = "my.custom.extension"
    // Set the spark.sql.extensions property before calling createSparkConf
    System.setProperty(SQL_EXTENSIONS_KEY, customExtension)

    try {
      val conf = FlintREPL.createSparkConf()
      assert(conf.get(SQL_EXTENSIONS_KEY) === customExtension)
    } finally {
      // Clean up the system property after the test
      System.clearProperty(SQL_EXTENSIONS_KEY)
    }
  }

  test("createHeartBeatUpdater should update heartbeat correctly") {
    // Mocks
    val threadPool = mock[ScheduledExecutorService]
    val scheduledFutureRaw = mock[ScheduledFuture[_]]
    val sessionManager = mock[SessionManager]
    val sessionId = "session1"
    // when scheduled task is scheduled, execute the runnable immediately only once and become no-op afterwards.
    when(threadPool
      .scheduleAtFixedRate(any[Runnable], *, *, eqTo(java.util.concurrent.TimeUnit.MILLISECONDS)))
      .thenAnswer((invocation: InvocationOnMock) => {
        val runnable = invocation.getArgument[Runnable](0)
        runnable.run()
        scheduledFutureRaw
      })

    // Invoke the method
    FlintREPL.createHeartBeatUpdater(sessionId, sessionManager, threadPool)

    // Verifications
    verify(sessionManager, atLeastOnce()).recordHeartbeat(sessionId)
  }

  test("PreShutdownListener updates FlintInstance if conditions are met") {
    // Mock dependencies
    val sessionId = "testSessionId"
    val timerContext = mock[Timer.Context]
    val sessionManager = mock[SessionManager]

    val interactiveSession = new InteractiveSession(
      "app123",
      "job123",
      sessionId,
      SessionStates.RUNNING,
      System.currentTimeMillis(),
      System.currentTimeMillis() - 10000)
    when(sessionManager.getSessionDetails(sessionId)).thenReturn(Some(interactiveSession))

    // Instantiate the listener
    val listener = new PreShutdownListener(sessionId, sessionManager, timerContext)

    // Simulate application end
    listener.onApplicationEnd(SparkListenerApplicationEnd(System.currentTimeMillis()))

    verify(sessionManager).updateSessionDetails(interactiveSession, SessionUpdateMode.UPDATE_IF)
    interactiveSession.state shouldBe SessionStates.DEAD
  }

  test("Test super.constructErrorDF should construct dataframe properly") {
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
        StructField("jobType", StringType, nullable = true),
        StructField("updateTime", LongType, nullable = false),
        StructField("queryRunTime", LongType, nullable = false)))

    val currentTime = System.currentTimeMillis()
    val queryRunTime = 3000L

    val error = "some error"
    val expectedRows = Seq(
      Row(
        null,
        null,
        jobId,
        applicationId,
        dataSourceName,
        "FAILED",
        error,
        "10",
        "select 1",
        "20",
        "interactive",
        currentTime,
        queryRunTime))
    val spark = SparkSession.builder().appName("Test").master("local").getOrCreate()
    spark.conf.set(FlintSparkConf.JOB_TYPE.key, FlintSparkConf.JOB_TYPE.defaultValue.get)
    val expected =
      spark.createDataFrame(spark.sparkContext.parallelize(expectedRows), expectedSchema)

    val flintStatement =
      new FlintStatement("failed", "select 1", "30", "10", LangType.SQL, currentTime, None)

    try {
      FlintREPL.currentTimeProvider = new MockTimeProvider(currentTime)

      // Compare the result
      val result =
        FlintREPL.handleCommandFailureAndGetFailedData(
          applicationId,
          jobId,
          spark,
          dataSourceName,
          error,
          flintStatement,
          "20",
          currentTime - queryRunTime)
      assertEqualDataframe(expected, result)
      assert(flintStatement.isFailed)
      assert(error == flintStatement.error.get)
    } finally {
      spark.close()
      FlintREPL.currentTimeProvider = new RealTimeProvider()

    }
  }

  test("test canPickNextStatement: Doc Exists and Valid JobId") {
    val sessionId = "session123"
    val jobId = "jobABC"
    val sessionManager = mock[SessionManager]

    val interactiveSession = new InteractiveSession(
      "app123",
      jobId,
      sessionId,
      SessionStates.RUNNING,
      System.currentTimeMillis(),
      System.currentTimeMillis() - 10000)
    when(sessionManager.getSessionDetails(sessionId)).thenReturn(Some(interactiveSession))

    val result = FlintREPL.canPickNextStatement(sessionId, sessionManager, jobId)

    assert(result)
  }

  test("test canPickNextStatement: Doc Exists but Different JobId") {
    val sessionId = "session123"
    val jobId = "jobABC"
    val differentJobId = "jobXYZ"
    val sessionManager = mock[SessionManager]

    val interactiveSession = new InteractiveSession(
      "app123",
      jobId,
      sessionId,
      SessionStates.RUNNING,
      System.currentTimeMillis(),
      System.currentTimeMillis() - 10000)

    when(sessionManager.getSessionDetails(sessionId)).thenReturn(Some(interactiveSession))

    // Execute the method under test
    val result = FlintREPL.canPickNextStatement(sessionId, sessionManager, differentJobId)

    // Assertions
    assert(!result) // The function should return false
  }

  test("test canPickNextStatement: Doc Exists, JobId Matches, but JobId is Excluded") {
    val sessionId = "session123"
    val jobId = "jobABC"
    val sessionManager = mock[SessionManager]

    val interactiveSession = new InteractiveSession(
      "app123",
      jobId,
      sessionId,
      SessionStates.RUNNING,
      System.currentTimeMillis(),
      System.currentTimeMillis() - 10000,
      Seq(jobId) // Add the jobId to the list to simulate exclusion
    )
    when(sessionManager.getSessionDetails(sessionId)).thenReturn(Some(interactiveSession))

    // Execute the method under test
    val result = FlintREPL.canPickNextStatement(sessionId, sessionManager, jobId)

    // Assertions
    assert(!result) // The function should return false because jobId is excluded
  }

  test("test canPickNextStatement: Doc Exists but Source is Null") {
    val sessionId = "session123"
    val jobId = "jobABC"
    val sessionManager = mock[SessionManager]

    when(sessionManager.getSessionDetails(sessionId)).thenReturn(None)

    // Execute the method under test
    val result = FlintREPL.canPickNextStatement(sessionId, sessionManager, jobId)

    // Assertions
    assert(result) // The function should return true despite the null source
  }

  test("test canPickNextStatement: Doc Exists with Unexpected Type in excludeJobIds") {
    val sessionId = "session123"
    val jobId = "jobABC"
    val mockOSClient = mock[OSClient]
    val sessionIndex = "sessionIndex"

    val mockSparkSession = mock[SparkSession]
    val mockConf = mock[RuntimeConfig]
    when(mockSparkSession.conf).thenReturn(mockConf)
    when(mockSparkSession.conf.get(FlintSparkConf.REQUEST_INDEX.key, ""))
      .thenReturn(sessionIndex)

    val sessionManager = new SessionManagerImpl(mockSparkSession, Some("resultIndex")) {
      override val osClient: OSClient = mockOSClient
    }

    val getResponse = mock[GetResponse]
    when(mockOSClient.getDoc(sessionIndex, sessionId)).thenReturn(getResponse)
    when(getResponse.isExists()).thenReturn(true)

    val sourceMap = new java.util.HashMap[String, Object]()
    sourceMap.put("jobId", jobId)
    sourceMap.put(
      "excludeJobIds",
      Integer.valueOf(123)
    ) // Using an Integer here to represent an unexpected type

    when(getResponse.getSourceAsMap).thenReturn(sourceMap)

    val result = FlintREPL.canPickNextStatement(sessionId, sessionManager, jobId)

    assert(result) // The function should return true
  }

  test("test canPickNextStatement: Doc Does Not Exist") {
    val sessionId = "session123"
    val jobId = "jobABC"
    val mockOSClient = mock[OSClient]
    val sessionIndex = "sessionIndex"
    val mockSparkSession = mock[SparkSession]
    val mockConf = mock[RuntimeConfig]
    when(mockSparkSession.conf).thenReturn(mockConf)
    when(mockSparkSession.conf.get(FlintSparkConf.REQUEST_INDEX.key, ""))
      .thenReturn(sessionIndex)

    val sessionManager = new SessionManagerImpl(mockSparkSession, Some("resultIndex")) {
      override val osClient: OSClient = mockOSClient
    }

    // Set up the mock GetResponse
    val getResponse = mock[GetResponse]
    when(mockOSClient.getDoc(sessionIndex, sessionId)).thenReturn(getResponse)
    when(getResponse.isExists()).thenReturn(false) // Simulate the document does not exist

    // Execute the function under test
    val result = FlintREPL.canPickNextStatement(sessionId, sessionManager, jobId)

    // Assert the function returns true
    assert(result)
  }

  test("test canPickNextStatement: OSClient Throws Exception") {
    val sessionId = "session123"
    val jobId = "jobABC"
    val mockOSClient = mock[OSClient]
    val sessionIndex = "sessionIndex"
    val mockSparkSession = mock[SparkSession]
    val mockConf = mock[RuntimeConfig]
    when(mockSparkSession.conf).thenReturn(mockConf)
    when(mockSparkSession.conf.get(FlintSparkConf.REQUEST_INDEX.key, ""))
      .thenReturn(sessionIndex)

    val sessionManager = new SessionManagerImpl(mockSparkSession, Some("resultIndex")) {
      override val osClient: OSClient = mockOSClient
    }

    // Set up the mock OSClient to throw an exception
    when(mockOSClient.getDoc(sessionIndex, sessionId))
      .thenThrow(new RuntimeException("OpenSearch cluster unresponsive"))

    // Execute the method under test and expect true, since the method is designed to return true even in case of an exception
    val result = FlintREPL.canPickNextStatement(sessionId, sessionManager, jobId)

    // Verify the result is true despite the exception
    assert(result)
  }

  test(
    "test canPickNextStatement: Doc Exists and excludeJobIds is a Single String Not Matching JobId") {
    val sessionId = "session123"
    val jobId = "jobABC"
    val nonMatchingExcludeJobId = "jobXYZ" // This ID does not match the jobId
    val mockOSClient = mock[OSClient]
    val sessionIndex = "sessionIndex"
    val mockSparkSession = mock[SparkSession]
    val mockConf = mock[RuntimeConfig]
    when(mockSparkSession.conf).thenReturn(mockConf)
    when(mockSparkSession.conf.get(FlintSparkConf.REQUEST_INDEX.key, ""))
      .thenReturn(sessionIndex)

    val sessionManager = new SessionManagerImpl(mockSparkSession, Some("resultIndex")) {
      override val osClient: OSClient = mockOSClient
    }

    val getResponse = mock[GetResponse]
    when(mockOSClient.getDoc(sessionIndex, sessionId)).thenReturn(getResponse)
    when(getResponse.isExists()).thenReturn(true)

    // Create a sourceMap with excludeJobIds as a String that does NOT match jobId
    val sourceMap = new java.util.HashMap[String, Object]()
    sourceMap.put("jobId", jobId.asInstanceOf[Object])
    sourceMap.put("excludeJobIds", nonMatchingExcludeJobId.asInstanceOf[Object])

    when(getResponse.getSourceAsMap).thenReturn(sourceMap)

    // Execute the method under test
    val result = FlintREPL.canPickNextStatement(sessionId, sessionManager, jobId)

    // The function should return true since jobId is not excluded
    assert(result)
  }

  test("processQueryException should handle exceptions, fail the command, and set the error") {
    val exception = new AccessDeniedException(
      "Unable to verify existence of default database: com.amazonaws.services.glue.model.AccessDeniedException: " +
        "User: ****** is not authorized to perform: glue:GetDatabase on resource: ****** " +
        "because no identity-based policy allows the glue:GetDatabase action")
    exception.setStatusCode(400)
    exception.setErrorCode("AccessDeniedException")
    exception.setServiceName("AWSGlue")

    val mockFlintStatement = mock[FlintStatement]
    val expectedError = (
      """{"Message":"Fail to read data from Glue. Cause: Access denied in AWS Glue service. Please check permissions. (Service: AWSGlue; """ +
        """Status Code: 400; Error Code: AccessDeniedException; Request ID: null; Proxy: null)",""" +
        """"ErrorSource":"AWSGlue","StatusCode":"400"}"""
    )

    val result = FlintREPL.processQueryException(exception, mockFlintStatement)

    result shouldEqual expectedError
    verify(mockFlintStatement).fail()
    verify(mockFlintStatement).error = Some(expectedError)

    assert(result == expectedError)
  }

  test("processQueryException should handle MetaException with AccessDeniedException properly") {
    val mockFlintCommand = mock[FlintStatement]

    // Simulate the root cause being MetaException
    val exception = new org.apache.hadoop.hive.metastore.api.MetaException(
      "AWSCatalogMetastoreClient: Unable to verify existence of default database: com.amazonaws.services.glue.model.AccessDeniedException: User: ****** is not authorized to perform: ******")

    val result = FlintREPL.processQueryException(exception, mockFlintCommand)

    val expectedError =
      """{"Message":"Fail to run query. Cause: Access denied in AWS Glue service. Please check permissions."}"""

    result shouldEqual expectedError
    verify(mockFlintCommand).fail()
    verify(mockFlintCommand).error = Some(expectedError)
  }

  test("Doc Exists and excludeJobIds is an ArrayList Containing JobId") {
    val sessionId = "session123"
    val jobId = "jobABC"
    val mockOSClient = mock[OSClient]
    val sessionIndex = "sessionIndex"
    val lastUpdateTime = System.currentTimeMillis()
    val mockSparkSession = mock[SparkSession]
    val mockConf = mock[RuntimeConfig]
    when(mockSparkSession.conf).thenReturn(mockConf)
    when(mockSparkSession.conf.get(FlintSparkConf.REQUEST_INDEX.key, ""))
      .thenReturn(sessionIndex)

    val sessionManager = new SessionManagerImpl(mockSparkSession, Some("resultIndex")) {
      override val osClient: OSClient = mockOSClient
    }

    val getResponse = mock[GetResponse]
    when(mockOSClient.getDoc(sessionIndex, sessionId)).thenReturn(getResponse)
    when(getResponse.isExists()).thenReturn(true)

    // Create a sourceMap with excludeJobIds as an ArrayList containing jobId
    val sourceMap = new java.util.HashMap[String, Object]()
    sourceMap.put("applicationId", applicationId.asInstanceOf[Object])
    sourceMap.put("state", "running".asInstanceOf[Object])
    sourceMap.put("jobId", jobId.asInstanceOf[Object])
    sourceMap.put("sessionId", sessionId.asInstanceOf[Object])
    sourceMap.put("lastUpdateTime", lastUpdateTime.asInstanceOf[Object])

    // Creating an ArrayList and adding the jobId to it
    val excludeJobIdsList = new java.util.ArrayList[String]()
    excludeJobIdsList.add(jobId)
    sourceMap.put("excludeJobIds", excludeJobIdsList.asInstanceOf[Object])

    when(getResponse.getSourceAsMap).thenReturn(sourceMap)
    // Execute the method under test
    val result = FlintREPL.canPickNextStatement(sessionId, sessionManager, jobId)

    // The function should return false since jobId is excluded
    assert(!result)
  }

  test("Doc Exists and excludeJobIds is an ArrayList Not Containing JobId") {
    val sessionId = "session123"
    val jobId = "jobABC"
    val mockOSClient = mock[OSClient]
    val sessionIndex = "sessionIndex"
    val lastUpdateTime = System.currentTimeMillis()
    val mockSparkSession = mock[SparkSession]
    val mockConf = mock[RuntimeConfig]
    when(mockSparkSession.conf).thenReturn(mockConf)
    when(mockSparkSession.conf.get(FlintSparkConf.REQUEST_INDEX.key, ""))
      .thenReturn(sessionIndex)

    val sessionManager = new SessionManagerImpl(mockSparkSession, Some("resultIndex")) {
      override val osClient: OSClient = mockOSClient
    }

    val getResponse = mock[GetResponse]
    when(mockOSClient.getDoc(sessionIndex, sessionId)).thenReturn(getResponse)
    when(getResponse.isExists()).thenReturn(true)

    // Create a sourceMap with excludeJobIds as an ArrayList not containing jobId
    val sourceMap = new java.util.HashMap[String, Object]()
    sourceMap.put("applicationId", applicationId.asInstanceOf[Object])
    sourceMap.put("state", "running".asInstanceOf[Object])
    sourceMap.put("jobId", jobId.asInstanceOf[Object])
    sourceMap.put("sessionId", sessionId.asInstanceOf[Object])
    sourceMap.put("lastUpdateTime", lastUpdateTime.asInstanceOf[Object])

    // Creating an ArrayList and adding a different jobId to it
    val excludeJobIdsList = new java.util.ArrayList[String]()
    excludeJobIdsList.add("jobXYZ") // This ID does not match the jobId
    sourceMap.put("excludeJobIds", excludeJobIdsList.asInstanceOf[Object])

    when(getResponse.getSourceAsMap).thenReturn(sourceMap)

    // Execute the method under test
    val result = FlintREPL.canPickNextStatement(sessionId, sessionManager, jobId)

    // The function should return true since the jobId is not in the excludeJobIds list
    assert(result)
  }

  test("exponentialBackoffRetry should retry on ConnectException") {
    val mockReader = mock[OpenSearchReader]
    val exception = new RuntimeException(
      new ConnectException(
        "Timeout connecting to [search-foo-1-bar.eu-west-1.es.amazonaws.com:443]"))
    val mockOSClient = mock[OSClient]
    when(
      mockOSClient.createQueryReader(any[String], any[String], any[String], eqTo(SortOrder.ASC)))
      .thenReturn(mockReader)
    when(mockReader.hasNext).thenThrow(exception)

    when(mockOSClient.getIndexMetadata(any[String])).thenReturn(FlintREPL.resultIndexMapping)

    val maxRetries = 1
    var actualRetries = 0

    val dataSource = "testDataSource"
    val sessionId = "testSessionId"
    val jobId = "testJobId"
    val applicationId = "testApplicationId"
    val sessionIndex = "sessionIndex"
    val lastUpdateTime = System.currentTimeMillis()

    val getResponse = mock[GetResponse]
    when(mockOSClient.getDoc(sessionIndex, sessionId)).thenReturn(getResponse)
    when(getResponse.isExists()).thenReturn(true)

    // Create a sourceMap with excludeJobIds as an ArrayList not containing jobId
    val sourceMap = new java.util.HashMap[String, Object]()
    sourceMap.put("applicationId", applicationId.asInstanceOf[Object])
    sourceMap.put("state", "running".asInstanceOf[Object])
    sourceMap.put("jobId", jobId.asInstanceOf[Object])
    sourceMap.put("sessionId", sessionId.asInstanceOf[Object])
    sourceMap.put("lastUpdateTime", lastUpdateTime.asInstanceOf[Object])

    val spark = SparkSession.builder().master("local").appName("FlintREPLTest").getOrCreate()
    try {
      spark.conf.set(FlintSparkConf.REQUEST_INDEX.key, sessionIndex)
      val sessionManager = new SessionManagerImpl(spark, Some("resultIndex")) {
        override val osClient: OSClient = mockOSClient
      }

      val queryResultWriter = mock[QueryResultWriter]

      val commandContext = CommandContext(
        applicationId,
        jobId,
        spark,
        dataSource,
        INTERACTIVE_JOB_TYPE,
        sessionId,
        sessionManager,
        queryResultWriter,
        Duration(10, MINUTES),
        60,
        60,
        DEFAULT_QUERY_LOOP_EXECUTION_FREQUENCY)

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
    val mockConf = mock[RuntimeConfig]
    when(mockSparkSession.conf).thenReturn(mockConf)
    when(mockSparkSession.conf.get(FlintSparkConf.JOB_TYPE.key))
      .thenReturn(FlintSparkConf.JOB_TYPE.defaultValue.get)
    when(mockSparkSession.conf.get(FlintSparkConf.REQUEST_INDEX.key, ""))
      .thenReturn("someSessionIndex")

    // val mockExecutionContextExecutor: ExecutionContextExecutor = mock[ExecutionContextExecutor]
    val threadPool = ThreadUtils.newDaemonThreadPoolScheduledExecutor("flint-repl", 1)
    implicit val executionContext = ExecutionContext.fromExecutor(threadPool)
    try {
      val dataSource = "someDataSource"
      val sessionId = "someSessionId"
      val startTime = System.currentTimeMillis()
      val expectedDataFrame = mock[DataFrame]
      val flintStatement =
        new FlintStatement(
          "running",
          "select 1",
          "30",
          "10",
          LangType.SQL,
          Instant.now().toEpochMilli(),
          None)
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

      val sessionManager = new SessionManagerImpl(mockSparkSession, Some("resultIndex"))
      val queryResultWriter = mock[QueryResultWriter]
      val commandContext = CommandContext(
        applicationId,
        jobId,
        mockSparkSession,
        dataSource,
        INTERACTIVE_JOB_TYPE,
        sessionId,
        sessionManager,
        queryResultWriter,
        Duration(10, MINUTES),
        60,
        60,
        DEFAULT_QUERY_LOOP_EXECUTION_FREQUENCY)
      val statementExecutionManager = new StatementExecutionManagerImpl(commandContext)

      val result = FlintREPL.executeAndHandle(
        applicationId,
        jobId,
        mockSparkSession,
        flintStatement,
        statementExecutionManager,
        dataSource,
        sessionId,
        executionContext,
        startTime,
        Duration(1, SECONDS),
        600000)

      verify(mockSparkSession, times(1)).sql(any[String])
      verify(sparkContext, times(1)).cancelJobGroup(any[String])
      assert("timeout" == flintStatement.state)
      assert(s"Executing ${flintStatement.query} timed out" == flintStatement.error.get)
      result should not be None
    } finally threadPool.shutdown()
  }

  test("executeAndHandle should handle ParseException properly") {
    val mockSparkSession = mock[SparkSession]
    val mockConf = mock[RuntimeConfig]
    when(mockSparkSession.conf).thenReturn(mockConf)
    when(mockSparkSession.conf.get(FlintSparkConf.JOB_TYPE.key))
      .thenReturn(FlintSparkConf.JOB_TYPE.defaultValue.get)
    when(mockSparkSession.conf.get(FlintSparkConf.REQUEST_INDEX.key, ""))
      .thenReturn("someSessionIndex")

    val flintStatement =
      new FlintStatement(
        "Running",
        "select * from default.http_logs limit1 1",
        "10",
        "20",
        LangType.SQL,
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

      val sessionManager = new SessionManagerImpl(mockSparkSession, Some("resultIndex"))
      val queryResultWriter = mock[QueryResultWriter]
      val commandContext = CommandContext(
        applicationId,
        jobId,
        mockSparkSession,
        dataSource,
        INTERACTIVE_JOB_TYPE,
        sessionId,
        sessionManager,
        queryResultWriter,
        Duration(10, MINUTES),
        60,
        60,
        DEFAULT_QUERY_LOOP_EXECUTION_FREQUENCY)
      val statementExecutionManager = new StatementExecutionManagerImpl(commandContext)

      val result = FlintREPL.executeAndHandle(
        applicationId,
        jobId,
        mockSparkSession,
        flintStatement,
        statementExecutionManager,
        dataSource,
        sessionId,
        executionContext,
        startTime,
        Duration.Inf,
        600000)

      // Verify that ParseException was caught and handled
      result should not be None // or result.isDefined shouldBe true
      flintStatement.error should not be None
      flintStatement.error.get should include("Syntax error:")
    } finally threadPool.shutdown()
  }

  test("setupFlintJobWithExclusionCheck should proceed normally when no jobs are excluded") {
    val sessionIndex = "sessionIndex"
    val sessionId = "session1"
    val mockSparkSession = mock[SparkSession]
    val mockConf = mock[RuntimeConfig]
    when(mockSparkSession.conf).thenReturn(mockConf)
    when(mockSparkSession.conf.get(FlintSparkConf.REQUEST_INDEX.key, ""))
      .thenReturn(sessionIndex)

    val mockOSClient = mock[OSClient]
    val getResponse = mock[GetResponse]
    when(mockOSClient.getDoc(sessionIndex, sessionId)).thenReturn(getResponse)
    when(getResponse.isExists()).thenReturn(true)

    val mockOpenSearchUpdater = mock[OpenSearchUpdater]
    doNothing().when(mockOpenSearchUpdater).upsert(any[String], any[String])

    val sessionManager = new SessionManagerImpl(mockSparkSession, Some("resultIndex")) {
      override val osClient: OSClient = mockOSClient
      override lazy val flintSessionIndexUpdater: OpenSearchUpdater = mockOpenSearchUpdater
    }
    val conf = new SparkConf().set("spark.flint.deployment.excludeJobs", "")

    when(mockOSClient.getDoc(*, *)).thenReturn(getResponse)
    when(getResponse.isExists()).thenReturn(true)
    when(getResponse.getSourceAsMap).thenReturn(
      Map[String, Object](
        "applicationId" -> "app1",
        "jobId" -> "job1",
        "sessionId" -> sessionId,
        "lastUpdateTime" -> java.lang.Long.valueOf(12345L),
        "error" -> "someError",
        "state" -> "running",
        "jobStartTime" -> java.lang.Long.valueOf(0L)).asJava)
    when(getResponse.getSeqNo).thenReturn(0L)
    when(getResponse.getPrimaryTerm).thenReturn(0L)

    // other mock objects like osClient, flintSessionIndexUpdater with necessary mocking
    val result = FlintREPL.setupFlintJobWithExclusionCheck(
      conf,
      "session1",
      jobId,
      applicationId,
      sessionManager,
      System.currentTimeMillis())
    assert(!result) // Expecting false as the job should proceed normally
  }

  test("setupFlintJobWithExclusionCheck should exit early if current job is excluded") {
    val sessionIndex = "sessionIndex"
    val sessionId = "session1"
    val mockSparkSession = mock[SparkSession]
    val mockConf = mock[RuntimeConfig]
    when(mockSparkSession.conf).thenReturn(mockConf)
    when(mockSparkSession.conf.get(FlintSparkConf.REQUEST_INDEX.key, ""))
      .thenReturn(sessionIndex)

    val mockOSClient = mock[OSClient]
    val getResponse = mock[GetResponse]
    when(mockOSClient.getDoc(sessionIndex, sessionId)).thenReturn(getResponse)
    when(getResponse.isExists()).thenReturn(true)

    val mockOpenSearchUpdater = mock[OpenSearchUpdater]
    doNothing().when(mockOpenSearchUpdater).upsert(any[String], any[String])

    val sessionManager = new SessionManagerImpl(mockSparkSession, Some("resultIndex")) {
      override val osClient: OSClient = mockOSClient
      override lazy val flintSessionIndexUpdater: OpenSearchUpdater = mockOpenSearchUpdater
    }

    when(mockOSClient.getDoc(*, *)).thenReturn(getResponse)
    when(getResponse.isExists()).thenReturn(true)
    // Mock the rest of the GetResponse as needed

    val conf = new SparkConf().set("spark.flint.deployment.excludeJobs", jobId)

    val result = FlintREPL.setupFlintJobWithExclusionCheck(
      conf,
      sessionId,
      jobId,
      applicationId,
      sessionManager,
      System.currentTimeMillis())
    assert(result) // Expecting true as the job should exit early
  }

  test("setupFlintJobWithExclusionCheck should exit early if a duplicate job is running") {
    val sessionIndex = "sessionIndex"
    val sessionId = "session1"
    val mockSparkSession = mock[SparkSession]
    val mockConf = mock[RuntimeConfig]
    when(mockSparkSession.conf).thenReturn(mockConf)
    when(mockSparkSession.conf.get(FlintSparkConf.REQUEST_INDEX.key, ""))
      .thenReturn(sessionIndex)

    val mockOSClient = mock[OSClient]
    val getResponse = mock[GetResponse]
    when(mockOSClient.getDoc(sessionIndex, sessionId)).thenReturn(getResponse)
    when(getResponse.isExists()).thenReturn(true)

    val mockOpenSearchUpdater = mock[OpenSearchUpdater]
    doNothing().when(mockOpenSearchUpdater).upsert(any[String], any[String])

    val sessionManager = new SessionManagerImpl(mockSparkSession, Some("resultIndex")) {
      override val osClient: OSClient = mockOSClient
      override lazy val flintSessionIndexUpdater: OpenSearchUpdater = mockOpenSearchUpdater
    }

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

    val conf = new SparkConf().set("spark.flint.deployment.excludeJobs", "job-1,job-2")

    val result = FlintREPL.setupFlintJobWithExclusionCheck(
      conf,
      sessionId,
      jobId,
      applicationId,
      sessionManager,
      System.currentTimeMillis())
    assert(result) // Expecting true for early exit due to duplicate job
  }

  test("setupFlintJobWithExclusionCheck should setup job normally when conditions are met") {
    val sessionIndex = "sessionIndex"
    val sessionId = "session1"
    val mockSparkSession = mock[SparkSession]
    val mockConf = mock[RuntimeConfig]
    when(mockSparkSession.conf).thenReturn(mockConf)
    when(mockSparkSession.conf.get(FlintSparkConf.REQUEST_INDEX.key, ""))
      .thenReturn(sessionIndex)

    val mockOSClient = mock[OSClient]
    val getResponse = mock[GetResponse]
    when(mockOSClient.getDoc(sessionIndex, sessionId)).thenReturn(getResponse)
    when(getResponse.isExists()).thenReturn(true)
    when(getResponse.getSourceAsMap).thenReturn(
      Map[String, Object](
        "applicationId" -> applicationId,
        "jobId" -> jobId,
        "sessionId" -> sessionId,
        "lastUpdateTime" -> java.lang.Long.valueOf(12345L),
        "error" -> "someError",
        "state" -> "running",
        "jobStartTime" -> java.lang.Long.valueOf(0L)).asJava)

    val mockOpenSearchUpdater = mock[OpenSearchUpdater]
    doNothing().when(mockOpenSearchUpdater).upsert(any[String], any[String])

    val sessionManager = new SessionManagerImpl(mockSparkSession, Some("resultIndex")) {
      override val osClient: OSClient = mockOSClient
      override lazy val flintSessionIndexUpdater: OpenSearchUpdater = mockOpenSearchUpdater
    }

    val conf = new SparkConf().set("spark.flint.deployment.excludeJobs", "job-3,job-4")

    val result = FlintREPL.setupFlintJobWithExclusionCheck(
      conf,
      sessionId,
      jobId,
      applicationId,
      sessionManager,
      System.currentTimeMillis())
    assert(!result) // Expecting false as the job proceeds normally
  }

  test("queryLoop continue until inactivity limit is reached") {
    val resultIndex = "testResultIndex"
    val dataSource = "testDataSource"
    val sessionIndex = "testSessionIndex"
    val sessionId = "testSessionId"

    val mockReader = mock[FlintReader]
    val mockOSClient = mock[OSClient]
    val getResponse = mock[GetResponse]

    when(mockOSClient.getDoc(sessionIndex, sessionId)).thenReturn(getResponse)
    when(getResponse.isExists()).thenReturn(true)
    when(getResponse.getSourceAsMap).thenReturn(
      Map[String, Object](
        "applicationId" -> applicationId,
        "jobId" -> jobId,
        "sessionId" -> sessionId,
        "lastUpdateTime" -> java.lang.Long.valueOf(12345L),
        "error" -> "someError",
        "state" -> "running",
        "jobStartTime" -> java.lang.Long.valueOf(0L)).asJava)

    when(
      mockOSClient.createQueryReader(any[String], any[String], any[String], eqTo(SortOrder.ASC)))
      .thenReturn(mockReader)
    when(mockReader.hasNext).thenReturn(false)
    when(mockOSClient.doesIndexExist(*)).thenReturn(true)
    when(mockOSClient.getIndexMetadata(*)).thenReturn(FlintREPL.resultIndexMapping)

    val shortInactivityLimit = 500 // 500 milliseconds

    // Create a SparkSession for testing
    val spark = SparkSession.builder().master("local").appName("FlintREPLTest").getOrCreate()

    spark.conf.set(FlintSparkConf.REQUEST_INDEX.key, sessionIndex)
    val sessionManager = new SessionManagerImpl(spark, Some(resultIndex)) {
      override val osClient: OSClient = mockOSClient
    }
    val queryResultWriter = mock[QueryResultWriter]

    val commandContext = CommandContext(
      applicationId,
      jobId,
      spark,
      dataSource,
      INTERACTIVE_JOB_TYPE,
      sessionId,
      sessionManager,
      queryResultWriter,
      Duration(10, MINUTES),
      shortInactivityLimit,
      60,
      DEFAULT_QUERY_LOOP_EXECUTION_FREQUENCY)

    val startTime = System.currentTimeMillis()

    FlintREPL.queryLoop(commandContext)

    val endTime = System.currentTimeMillis()

    // Check if the loop ran for approximately the duration of the inactivity limit
    assert(endTime - startTime >= shortInactivityLimit)

    // Stop the SparkSession
    spark.stop()
  }

  test("queryLoop should stop when canPickUpNextStatement is false") {
    val resultIndex = "testResultIndex"
    val dataSource = "testDataSource"
    val sessionIndex = "testSessionIndex"
    val sessionId = "testSessionId"

    val mockReader = mock[FlintReader]
    val mockOSClient = mock[OSClient]

    // Mocking canPickNextStatement to return false
    when(mockOSClient.getDoc(sessionIndex, sessionId)).thenAnswer(_ => {
      val mockGetResponse = mock[GetResponse]
      when(mockGetResponse.isExists()).thenReturn(true)
      when(mockGetResponse.getSourceAsMap).thenReturn(
        Map[String, Object](
          "applicationId" -> applicationId,
          "jobId" -> "differentJobId",
          "sessionId" -> sessionId,
          "lastUpdateTime" -> java.lang.Long.valueOf(12345L),
          "error" -> "someError",
          "state" -> "running",
          "jobStartTime" -> java.lang.Long.valueOf(0L)).asJava)
      mockGetResponse
    })

    when(
      mockOSClient.createQueryReader(any[String], any[String], any[String], eqTo(SortOrder.ASC)))
      .thenReturn(mockReader)
    when(mockReader.hasNext).thenReturn(true)
    when(mockOSClient.doesIndexExist(*)).thenReturn(true)
    when(mockOSClient.getIndexMetadata(*)).thenReturn(FlintREPL.resultIndexMapping)

    val longInactivityLimit = 10000 // 10 seconds

    // Create a SparkSession for testing
    val spark = SparkSession.builder().master("local").appName("FlintREPLTest").getOrCreate()

    spark.conf.set(FlintSparkConf.REQUEST_INDEX.key, sessionIndex)
    val sessionManager = new SessionManagerImpl(spark, Some(resultIndex)) {
      override val osClient: OSClient = mockOSClient
    }
    val queryResultWriter = mock[QueryResultWriter]

    val commandContext = CommandContext(
      applicationId,
      jobId,
      spark,
      dataSource,
      INTERACTIVE_JOB_TYPE,
      sessionId,
      sessionManager,
      queryResultWriter,
      Duration(10, MINUTES),
      longInactivityLimit,
      60,
      DEFAULT_QUERY_LOOP_EXECUTION_FREQUENCY)

    val startTime = System.currentTimeMillis()

    FlintREPL.queryLoop(commandContext)

    val endTime = System.currentTimeMillis()

    // Check if the loop stopped before the inactivity limit
    assert(endTime - startTime < longInactivityLimit)

    // Stop the SparkSession
    spark.stop()
  }

  test("queryLoop should properly shut down the thread pool after execution") {
    val resultIndex = "testResultIndex"
    val dataSource = "testDataSource"
    val sessionIndex = "testSessionIndex"
    val sessionId = "testSessionId"

    val mockReader = mock[FlintReader]
    val mockOSClient = mock[OSClient]
    when(
      mockOSClient.createQueryReader(any[String], any[String], any[String], eqTo(SortOrder.ASC)))
      .thenReturn(mockReader)
    when(mockReader.hasNext).thenReturn(false)
    when(mockOSClient.doesIndexExist(*)).thenReturn(true)
    when(mockOSClient.getIndexMetadata(*)).thenReturn(FlintREPL.resultIndexMapping)

    val getResponse = mock[GetResponse]
    when(mockOSClient.getDoc(sessionIndex, sessionId)).thenReturn(getResponse)
    when(getResponse.isExists()).thenReturn(true)
    when(getResponse.getSourceAsMap).thenReturn(
      Map[String, Object](
        "applicationId" -> applicationId,
        "jobId" -> jobId,
        "sessionId" -> sessionId,
        "lastUpdateTime" -> java.lang.Long.valueOf(12345L),
        "error" -> "someError",
        "state" -> "running",
        "jobStartTime" -> java.lang.Long.valueOf(0L)).asJava)

    val inactivityLimit = 500 // 500 milliseconds

    // Create a SparkSession for testing
    val spark = SparkSession.builder().master("local").appName("FlintREPLTest").getOrCreate()

    spark.conf.set(FlintSparkConf.REQUEST_INDEX.key, sessionIndex)
    val sessionManager = new SessionManagerImpl(spark, Some(resultIndex)) {
      override val osClient: OSClient = mockOSClient
    }
    val queryResultWriter = mock[QueryResultWriter]

    val commandContext = CommandContext(
      applicationId,
      jobId,
      spark,
      dataSource,
      INTERACTIVE_JOB_TYPE,
      sessionId,
      sessionManager,
      queryResultWriter,
      Duration(10, MINUTES),
      inactivityLimit,
      60,
      DEFAULT_QUERY_LOOP_EXECUTION_FREQUENCY)

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
    val resultIndex = "testResultIndex"
    val dataSource = "testDataSource"
    val sessionIndex = "testSessionIndex"
    val sessionId = "testSessionId"

    val mockReader = mock[FlintReader]
    val mockOSClient = mock[OSClient]
    val getResponse = mock[GetResponse]

    when(mockOSClient.getDoc(*, *)).thenReturn(getResponse)
    when(getResponse.isExists()).thenReturn(true)
    when(getResponse.getSourceAsMap).thenReturn(
      Map[String, Object](
        "applicationId" -> applicationId,
        "jobId" -> jobId,
        "sessionId" -> sessionId,
        "lastUpdateTime" -> java.lang.Long.valueOf(12345L),
        "error" -> "someError",
        "state" -> "running",
        "jobStartTime" -> java.lang.Long.valueOf(0L)).asJava)

    when(
      mockOSClient.createQueryReader(any[String], any[String], any[String], eqTo(SortOrder.ASC)))
      .thenReturn(mockReader)
    // Simulate an exception thrown when hasNext is called
    when(mockReader.hasNext).thenThrow(new RuntimeException("Test exception"))
    when(mockOSClient.doesIndexExist(*)).thenReturn(true)
    when(mockOSClient.getIndexMetadata(*)).thenReturn(FlintREPL.resultIndexMapping)

    val inactivityLimit = 500 // 500 milliseconds

    // Create a SparkSession for testing
    val spark = SparkSession.builder().master("local").appName("FlintREPLTest").getOrCreate()

    spark.conf.set(FlintSparkConf.REQUEST_INDEX.key, sessionIndex)
    val sessionManager = new SessionManagerImpl(spark, Some(resultIndex)) {
      override val osClient: OSClient = mockOSClient
    }
    val queryResultWriter = mock[QueryResultWriter]

    val commandContext = CommandContext(
      applicationId,
      jobId,
      spark,
      dataSource,
      INTERACTIVE_JOB_TYPE,
      sessionId,
      sessionManager,
      queryResultWriter,
      Duration(10, MINUTES),
      inactivityLimit,
      60,
      DEFAULT_QUERY_LOOP_EXECUTION_FREQUENCY)

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
    val resultIndex = "testResultIndex"
    val dataSource = "testDataSource"
    val sessionIndex = "testSessionIndex"
    val sessionId = "testSessionId"

    val mockReader = mock[FlintReader]
    val mockOSClient = mock[OSClient]
    when(
      mockOSClient.createQueryReader(any[String], any[String], any[String], eqTo(SortOrder.ASC)))
      .thenReturn(mockReader)
    val getResponse = mock[GetResponse]
    when(mockOSClient.getDoc(*, *)).thenReturn(getResponse)
    when(getResponse.isExists()).thenReturn(false)
    when(mockOSClient.doesIndexExist(*)).thenReturn(true)
    when(mockOSClient.getIndexMetadata(*)).thenReturn(FlintREPL.resultIndexMapping)

    val mockOpenSearchUpdater = mock[OpenSearchUpdater]
    doNothing().when(mockOpenSearchUpdater).upsert(any[String], any[String])

    // Configure mockReader to return true once and then false to exit the loop
    when(mockReader.hasNext).thenReturn(true).thenReturn(false)
    val command =
      """ {
          "state": "running",
          "query": "SELECT * FROM table",
          "statementId": "stmt123",
          "queryId": "query456",
          "lang": "sql",
          "submitTime": 1234567890,
          "error": "Some error"
        }
        """
    when(mockReader.next).thenReturn(command)

    val inactivityLimit = 5000 // 5 seconds

    // Create a SparkSession for testing\
    val mockSparkSession = mock[SparkSession]
    val expectedDataFrame = mock[DataFrame]
    when(mockSparkSession.createDataFrame(any[Seq[Product]])(any[TypeTag[Product]]))
      .thenReturn(expectedDataFrame)
    val sparkContext = mock[SparkContext]
    when(mockSparkSession.sparkContext).thenReturn(sparkContext)

    val mockConf = mock[RuntimeConfig]
    when(mockSparkSession.conf).thenReturn(mockConf)
    when(mockSparkSession.conf.get(FlintSparkConf.JOB_TYPE.key))
      .thenReturn(FlintSparkConf.JOB_TYPE.defaultValue.get)
    when(mockSparkSession.conf.get(FlintSparkConf.REQUEST_INDEX.key, ""))
      .thenReturn(sessionIndex)
    when(mockSparkSession.conf.get(FlintSparkConf.CUSTOM_STATEMENT_MANAGER.key, ""))
      .thenReturn("")

    when(expectedDataFrame.toDF(any[Seq[String]]: _*)).thenReturn(expectedDataFrame)

    val sessionManager = new SessionManagerImpl(mockSparkSession, Some(resultIndex)) {
      override val osClient: OSClient = mockOSClient
      override lazy val flintSessionIndexUpdater: OpenSearchUpdater = mockOpenSearchUpdater
    }
    val queryResultWriter = mock[QueryResultWriter]

    val commandContext = CommandContext(
      applicationId,
      jobId,
      mockSparkSession,
      dataSource,
      INTERACTIVE_JOB_TYPE,
      sessionId,
      sessionManager,
      queryResultWriter,
      Duration(10, MINUTES),
      inactivityLimit,
      60,
      DEFAULT_QUERY_LOOP_EXECUTION_FREQUENCY)

    val startTime = Instant.now().toEpochMilli()

    // Running the queryLoop
    FlintREPL.queryLoop(commandContext)

    val endTime = Instant.now().toEpochMilli()

    // Assuming processCommands updates the lastActivityTime to the current time
    assert(endTime - startTime >= inactivityLimit)

    val expectedCalls =
      Math.ceil(inactivityLimit.toDouble / DEFAULT_QUERY_LOOP_EXECUTION_FREQUENCY).toInt
    verify(mockOSClient, Mockito.atMost(expectedCalls)).getIndexMetadata(*)
  }

  val testCases = Table(
    ("inactivityLimit", "queryLoopExecutionFrequency"),
    (5000, 100L), // 5 seconds, 100 ms
    (100, 300L) // 100 ms, 300 ms
  )

  test(
    "queryLoop should execute loop without processing any commands for different inactivity limits and frequencies") {
    forAll(testCases) { (inactivityLimit, queryLoopExecutionFrequency) =>
      val resultIndex = "testResultIndex"
      val dataSource = "testDataSource"
      val sessionIndex = "testSessionIndex"
      val sessionId = "testSessionId"

      val mockReader = mock[FlintReader]
      val mockOSClient = mock[OSClient]
      when(
        mockOSClient
          .createQueryReader(any[String], any[String], any[String], eqTo(SortOrder.ASC)))
        .thenReturn(mockReader)
      val getResponse = mock[GetResponse]
      when(mockOSClient.getDoc(*, *)).thenReturn(getResponse)
      when(mockOSClient.doesIndexExist(*)).thenReturn(true)
      when(mockOSClient.getIndexMetadata(*)).thenReturn(FlintREPL.resultIndexMapping)

      when(getResponse.isExists()).thenReturn(false)
      when(getResponse.getSourceAsMap).thenReturn(
        Map[String, Object](
          "applicationId" -> applicationId,
          "jobId" -> jobId,
          "sessionId" -> sessionId,
          "lastUpdateTime" -> java.lang.Long.valueOf(12345L),
          "error" -> "someError",
          "state" -> "running",
          "jobStartTime" -> java.lang.Long.valueOf(0L)).asJava)

      when(mockReader.hasNext).thenReturn(false)

      // Create a SparkSession for testing
      val spark = SparkSession.builder().master("local").appName("FlintREPLTest").getOrCreate()

      spark.conf.set(FlintSparkConf.REQUEST_INDEX.key, sessionIndex)
      val sessionManager = new SessionManagerImpl(spark, Some(resultIndex)) {
        override val osClient: OSClient = mockOSClient
      }
      val queryResultWriter = mock[QueryResultWriter]

      val commandContext = CommandContext(
        applicationId,
        jobId,
        spark,
        dataSource,
        INTERACTIVE_JOB_TYPE,
        sessionId,
        sessionManager,
        queryResultWriter,
        Duration(10, MINUTES),
        inactivityLimit,
        60,
        queryLoopExecutionFrequency)

      val startTime = Instant.now().toEpochMilli()

      // Running the queryLoop
      FlintREPL.queryLoop(commandContext)

      val endTime = Instant.now().toEpochMilli()

      val elapsedTime = endTime - startTime

      // Assert that the loop ran for at least the duration of the inactivity limit
      assert(elapsedTime >= inactivityLimit)

      // Verify query execution frequency
      val expectedCalls = Math.ceil(elapsedTime.toDouble / queryLoopExecutionFrequency).toInt
      verify(mockReader, Mockito.atMost(expectedCalls)).hasNext

      // Verify that no command was actually processed
      verify(mockReader, never()).next()

      // Stop the SparkSession
      spark.stop()
    }
  }
}
