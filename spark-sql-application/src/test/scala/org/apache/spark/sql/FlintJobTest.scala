/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql

import java.net.ConnectException

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.duration.{Duration, MINUTES}

import org.mockito.ArgumentMatchersSugar
import org.mockito.Mockito.when
import org.opensearch.action.get.GetResponse
import org.opensearch.flint.core.storage.{FlintReader, OpenSearchReader}
import org.opensearch.search.sort.SortOrder
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.FlintREPLConfConstants.DEFAULT_QUERY_LOOP_EXECUTION_FREQUENCY
import org.apache.spark.sql.SparkConfConstants.{DEFAULT_SQL_EXTENSIONS, SQL_EXTENSIONS_KEY}
import org.apache.spark.sql.exception.UnrecoverableException
import org.apache.spark.sql.flint.config.FlintSparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.{CleanerFactory, MockTimeProvider}

class FlintJobTest
    extends SparkFunSuite
    with MockitoSugar
    with ArgumentMatchersSugar
    with JobMatchers {
  private val jobId = "testJobId"
  private val applicationId = "testApplicationId"
  private val ONE_EXECUTOR_SEGMENT = "1e"
  private val INTERACTIVE_JOB_TYPE = "interactive"

  val spark =
    SparkSession.builder().appName("Test").master("local").getOrCreate()
  spark.conf.set(FlintSparkConf.JOB_TYPE.key, "streaming")
  // Define input dataframe
  val inputSchema = StructType(
    Seq(
      StructField("Letter", StringType, nullable = false),
      StructField("Number", IntegerType, nullable = false)))
  val inputRows = Seq(Row("A", 1), Row("B", 2), Row("C", 3))
  val input: DataFrame =
    spark.createDataFrame(spark.sparkContext.parallelize(inputRows), inputSchema)

  test("Test getFormattedData method") {
    // Define expected dataframe
    val dataSourceName = "myGlueS3"
    val expectedSchema = StructType(
      Seq(
        StructField("result", ArrayType(StringType, containsNull = true), nullable = true),
        StructField("schema", ArrayType(StringType, containsNull = true), nullable = true),
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

    val currentTime: Long = System.currentTimeMillis()
    val queryRunTime: Long = 3000L

    val expectedRows = Seq(
      Row(
        Array(
          "{'Letter':'A','Number':1}",
          "{'Letter':'B','Number':2}",
          "{'Letter':'C','Number':3}"),
        Array(
          "{'column_name':'Letter','data_type':'string'}",
          "{'column_name':'Number','data_type':'integer'}"),
        jobId,
        applicationId,
        dataSourceName,
        "SUCCESS",
        "",
        "10",
        "select 1",
        "20",
        "streaming",
        currentTime,
        queryRunTime))
    val expected: DataFrame =
      spark.createDataFrame(spark.sparkContext.parallelize(expectedRows), expectedSchema)

    // Compare the result
    val result =
      FlintJob.getFormattedData(
        applicationId,
        jobId,
        input,
        spark,
        dataSourceName,
        "10",
        "select 1",
        "20",
        currentTime - queryRunTime,
        new MockTimeProvider(currentTime),
        CleanerFactory.cleaner(false))
    assertEqualDataframe(expected, result)
  }

  test("test isSuperset") {
    // Note in input false has enclosed double quotes, while mapping just has false
    val input =
      """{"dynamic":"false","properties":{"result":{"type":"object"},"schema":{"type":"object"},
        |"applicationId":{"type":"keyword"},"jobRunId":{
        |"type":"keyword"},"dataSourceName":{"type":"keyword"},"status":{"type":"keyword"},
        |"error":{"type":"text"}}}
        |""".stripMargin
    val mapping =
      """{"dynamic":"false","properties":{"result":{"type":"object"},"schema":{"type":"object"}, "jobType":{"type": "keyword"},
        |"applicationId":{"type":"keyword"},"jobRunId":{
        |"type":"keyword"},"dataSourceName":{"type":"keyword"},"status":{"type":"keyword"},
        |"error":{"type":"text"}}}
        |""".stripMargin

    // Assert that input is a superset of mapping
    assert(FlintJob.isSuperset(input, mapping))

    // Assert that mapping is a superset of input
    assert(FlintJob.isSuperset(mapping, input))
  }

  test("default streaming query maxExecutors is 10") {
    val conf = spark.sparkContext.conf
    FlintJob.configDYNMaxExecutors(conf, "streaming")
    conf.get("spark.dynamicAllocation.maxExecutors") shouldBe "10"
  }

  test("override streaming query maxExecutors") {
    spark.sparkContext.conf.set("spark.flint.streaming.dynamicAllocation.maxExecutors", "30")
    FlintJob.configDYNMaxExecutors(spark.sparkContext.conf, "streaming")
    spark.sparkContext.conf.get("spark.dynamicAllocation.maxExecutors") shouldBe "30"
  }

  test("createSparkConf should set the app name and default SQL extensions") {
    val conf = FlintJob.createSparkConf()

    // Assert that the app name is set correctly
    assert(conf.get("spark.app.name") === "FlintJob$")

    // Assert that the default SQL extensions are set correctly
    assert(conf.get(SQL_EXTENSIONS_KEY) === DEFAULT_SQL_EXTENSIONS)
  }

  test(
    "createSparkConf should not use defaultExtensions if spark.sql.extensions is already set") {
    val customExtension = "my.custom.extension"
    // Set the spark.sql.extensions property before calling createSparkConf
    System.setProperty(SQL_EXTENSIONS_KEY, customExtension)

    try {
      val conf = FlintJob.createSparkConf()
      assert(conf.get(SQL_EXTENSIONS_KEY) === customExtension)
    } finally {
      // Clean up the system property after the test
      System.clearProperty(SQL_EXTENSIONS_KEY)
    }
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

    when(mockOSClient.getIndexMetadata(any[String])).thenReturn(FlintJob.resultIndexMapping)

    val maxRetries = 1
    var actualRetries = 0

    val jobId = "testJobId"
    val applicationId = "testApplicationId"
    val sessionIndex = "sessionIndex"
    val resultIndexOption = Some("testResultIndex")
    val lastUpdateTime = System.currentTimeMillis()

    // Create a sourceMap with excludeJobIds as an ArrayList not containing jobId
    val sourceMap = new java.util.HashMap[String, Object]()
    sourceMap.put("applicationId", applicationId.asInstanceOf[Object])
    sourceMap.put("state", "running".asInstanceOf[Object])
    sourceMap.put("jobId", jobId.asInstanceOf[Object])
    sourceMap.put("lastUpdateTime", lastUpdateTime.asInstanceOf[Object])

    val spark = SparkSession.builder().master("local").appName("FlintREPLTest").getOrCreate()
    try {
      spark.conf.set(FlintSparkConf.REQUEST_INDEX.key, sessionIndex)
      val sessionManager = new SessionManagerImpl(spark, Some("resultIndex")) {
        override val osClient: OSClient = mockOSClient
      }

      val commandContext = CommandContext(
        applicationId,
        jobId,
        spark,
        "",
        "",
        "",
        sessionManager,
        Duration.Inf,
        60,
        60,
        DEFAULT_QUERY_LOOP_EXECUTION_FREQUENCY)

      intercept[RuntimeException] {
        FlintREPL.exponentialBackoffRetry(maxRetries, 2.seconds) {
          actualRetries += 1
          FlintJob.queryLoop(commandContext, ONE_EXECUTOR_SEGMENT)
        }
      }

      assert(actualRetries == maxRetries)
    } finally {
      // Stop the SparkSession
      spark.stop()
    }
  }

  test("queryLoop continue until inactivity limit is reached") {
    val resultIndex = "testResultIndex"
    val dataSource = "testDataSource"
    val sessionIndex = "testSessionIndex"
    val sessionId = "testSessionId"
    val resultIndexOption = Some("testResultIndex")

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

    val shortInactivityLimit = 50 // 50 milliseconds

    // Create a SparkSession for testing
    val spark = SparkSession.builder().master("local").appName("FlintREPLTest").getOrCreate()

    spark.conf.set(FlintSparkConf.REQUEST_INDEX.key, sessionIndex)
    val sessionManager = new SessionManagerImpl(spark, Some(resultIndex)) {
      override val osClient: OSClient = mockOSClient
    }

    val commandContext = CommandContext(
      applicationId,
      jobId,
      spark,
      dataSource,
      INTERACTIVE_JOB_TYPE,
      sessionId,
      sessionManager,
      Duration(10, MINUTES),
      shortInactivityLimit,
      60,
      DEFAULT_QUERY_LOOP_EXECUTION_FREQUENCY)

    val startTime = System.currentTimeMillis()

    FlintJob.queryLoop(commandContext, ONE_EXECUTOR_SEGMENT)

    val endTime = System.currentTimeMillis()

    // Check if the loop ran for approximately the duration of the inactivity limit
    assert(endTime - startTime >= shortInactivityLimit)

    // Stop the SparkSession
    spark.stop()
  }

  test("queryLoop should properly shut down the thread pool after execution") {
    val resultIndex = "testResultIndex"
    val dataSource = "testDataSource"
    val sessionIndex = "testSessionIndex"
    val sessionId = "testSessionId"
    val resultIndexOption = Some("testResultIndex")

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

    val commandContext = CommandContext(
      applicationId,
      jobId,
      spark,
      dataSource,
      INTERACTIVE_JOB_TYPE,
      sessionId,
      sessionManager,
      Duration(10, MINUTES),
      inactivityLimit,
      60,
      DEFAULT_QUERY_LOOP_EXECUTION_FREQUENCY)

    try {
      FlintJob.queryLoop(commandContext, ONE_EXECUTOR_SEGMENT)

    } finally {
      // Stop the SparkSession
      spark.stop()
    }
  }

  test("queryLoop handle exceptions within the loop gracefully") {
    val resultIndex = "testResultIndex"
    val dataSource = "testDataSource"
    val sessionIndex = "testSessionIndex"
    val sessionId = "testSessionId"
    val resultIndexOption = Some("testResultIndex")

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
    val unrecoverableException = UnrecoverableException(new RuntimeException("Test exception"))
    when(mockReader.hasNext).thenThrow(unrecoverableException)
    when(mockOSClient.doesIndexExist(*)).thenReturn(true)
    when(mockOSClient.getIndexMetadata(*)).thenReturn(FlintREPL.resultIndexMapping)

    val inactivityLimit = 500 // 500 milliseconds

    // Create a SparkSession for testing
    val spark = SparkSession.builder().master("local").appName("FlintREPLTest").getOrCreate()

    spark.conf.set(FlintSparkConf.REQUEST_INDEX.key, sessionIndex)
    val sessionManager = new SessionManagerImpl(spark, Some(resultIndex)) {
      override val osClient: OSClient = mockOSClient
    }

    val commandContext = CommandContext(
      applicationId,
      jobId,
      spark,
      dataSource,
      INTERACTIVE_JOB_TYPE,
      sessionId,
      sessionManager,
      Duration(10, MINUTES),
      inactivityLimit,
      60,
      DEFAULT_QUERY_LOOP_EXECUTION_FREQUENCY)

    try {
      intercept[UnrecoverableException] {
        FlintJob.queryLoop(commandContext, ONE_EXECUTOR_SEGMENT)
      }

      FlintJob.throwableHandler.exceptionThrown shouldBe Some(unrecoverableException)
    } finally {
      // Stop the SparkSession
      spark.stop()
    }
  }
}
