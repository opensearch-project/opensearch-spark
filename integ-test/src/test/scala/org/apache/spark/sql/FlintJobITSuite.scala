/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql

import java.util.{Base64, Collections}
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.{Duration, MINUTES}
import scala.util.{Failure, Success}

import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequest
import org.opensearch.action.get.GetRequest
import org.opensearch.client.RequestOptions
import org.opensearch.flint.core.FlintOptions
import org.opensearch.flint.spark.FlintSparkSuite
import org.opensearch.flint.spark.skipping.FlintSparkSkippingIndex.getSkippingIndexName
import org.scalatest.matchers.must.Matchers.{contain, defined}
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import org.apache.spark.sql.flint.FlintDataSourceV2.FLINT_DATASOURCE
import org.apache.spark.sql.flint.config.FlintSparkConf._
import org.apache.spark.sql.util.MockEnvironment
import org.apache.spark.util.ThreadUtils

class FlintJobITSuite extends FlintSparkSuite with JobTest {

  /** Test table and index name */
  private val testTable = "spark_catalog.default.skipping_sql_test"
  private val testIndex = getSkippingIndexName(testTable)
  val resultIndex = "query_results2"
  val appId = "00feq82b752mbt0p"
  val dataSourceName = "my_glue1"
  var osClient: OSClient = _
  val threadLocalFuture = new ThreadLocal[Future[Unit]]()

  override def beforeAll(): Unit = {
    super.beforeAll()
    // initialized after the container is started
    osClient = new OSClient(new FlintOptions(openSearchOptions.asJava))
    createPartitionedMultiRowAddressTable(testTable)
  }

  protected override def afterEach(): Unit = {
    super.afterEach()

    deleteTestIndex(testIndex)

    waitJobStop(threadLocalFuture.get())

    threadLocalFuture.remove()
  }

  def waitJobStop(future: Future[Unit]): Unit = {
    try {
      val activeJob = spark.streams.active.find(_.name == testIndex)
      if (activeJob.isDefined) {
        activeJob.get.stop()
      }
      ThreadUtils.awaitResult(future, Duration(1, MINUTES))
    } catch {
      case e: Exception =>
        e.printStackTrace()
        assert(false, "failure waiting for job to finish")
    }
  }

  def startJob(query: String, jobRunId: String): Future[Unit] = {
    val prefix = "flint-job-test"
    val threadPool = ThreadUtils.newDaemonThreadPoolScheduledExecutor(prefix, 1)
    implicit val executionContext = ExecutionContext.fromExecutor(threadPool)
    val streamingRunningCount = new AtomicInteger(0)

    val futureResult = Future {
      /*
       * Because we cannot test from FlintJob.main() for the reason below, we have to configure
       * all Spark conf required by Flint code underlying manually.
       */
      spark.conf.set(DATA_SOURCE_NAME.key, dataSourceName)
      spark.conf.set(JOB_TYPE.key, "batch")

      /**
       * FlintJob.main() is not called because we need to manually set these variables within a
       * JobOperator instance to accommodate specific runtime requirements.
       */
      val job =
        JobOperator(spark, query, dataSourceName, resultIndex, true, streamingRunningCount)
      job.envinromentProvider = new MockEnvironment(
        Map("SERVERLESS_EMR_JOB_ID" -> jobRunId, "SERVERLESS_EMR_VIRTUAL_CLUSTER_ID" -> appId))
      job.terminateJVM = false
      job.start()
    }
    futureResult.onComplete {
      case Success(result) => logInfo(s"Success result: $result")
      case Failure(ex) =>
        ex.printStackTrace()
        assert(false, s"An error has occurred: ${ex.getMessage}")
    }
    futureResult
  }

  test("create skipping index with auto refresh") {
    val query =
      s"""
         | CREATE SKIPPING INDEX ON $testTable
         | (
         |   year PARTITION,
         |   name VALUE_SET,
         |   age MIN_MAX
         | )
         | WITH (auto_refresh = true)
         | """.stripMargin
    val queryStartTime = System.currentTimeMillis()
    val jobRunId = "00ff4o3b5091080q"
    threadLocalFuture.set(startJob(query, jobRunId))

    val validation: REPLResult => Boolean = result => {
      assert(
        result.results.size == 0,
        s"expected result size is 0, but got ${result.results.size}")
      assert(
        result.schemas.size == 0,
        s"expected schema size is 0, but got ${result.schemas.size}")

      assert(result.status == "SUCCESS", s"expected status is SUCCESS, but got ${result.status}")
      assert(result.error.isEmpty, s"we don't expect error, but got ${result.error}")
      assert(result.queryId.isEmpty, s"we don't expect query id, but got ${result.queryId}")

      commonAssert(result, jobRunId, query, queryStartTime)
      true
    }
    pollForResultAndAssert(validation, jobRunId)

    val activeJob = spark.streams.active.find(_.name == testIndex)
    activeJob shouldBe defined
    failAfter(streamingTimeout) {
      activeJob.get.processAllAvailable()
    }
    val indexData = spark.read.format(FLINT_DATASOURCE).load(testIndex)
    flint.describeIndex(testIndex) shouldBe defined
    indexData.count() shouldBe 2
  }

  test("create skipping index with auto refresh and streaming job failure") {
    val query =
      s"""
         | CREATE SKIPPING INDEX ON $testTable
         | (
         |   year PARTITION,
         |   name VALUE_SET,
         |   age MIN_MAX
         | )
         | WITH (auto_refresh = true)
         | """.stripMargin
    val jobRunId = "00ff4o3b5091080q"
    threadLocalFuture.set(startJob(query, jobRunId))

    // Waiting from streaming job start and complete current batch
    pollForResultAndAssert(_ => true, jobRunId)
    val activeJob = spark.streams.active.find(_.name == testIndex)
    awaitStreamingComplete(activeJob.get.id.toString)

    try {
      // Set Flint index readonly to simulate streaming job exception
      setFlintIndexReadOnly(true)

      // Trigger a new micro batch execution
      sql(s"""
           | INSERT INTO $testTable
           | PARTITION (year=2023, month=6)
           | SELECT *
           | FROM VALUES ('Test', 35, 'Seattle')
           |""".stripMargin)
      try {
        awaitStreamingComplete(activeJob.get.id.toString)
      } catch {
        case _: Exception => // expected
      }

      // Assert Flint index transitioned to FAILED state
      val latestId = Base64.getEncoder.encodeToString(testIndex.getBytes)
      latestLogEntry(latestId) should contain("state" -> "failed")
    } finally {
      // Reset so Flint index can be cleaned up in afterEach
      setFlintIndexReadOnly(false)
    }
  }

  test("create skipping index with non-existent table") {
    val query =
      s"""
         | CREATE SKIPPING INDEX ON testTable
         | (
         |   year PARTITION,
         |   name VALUE_SET,
         |   age MIN_MAX
         | )
         | WITH (auto_refresh = true)
         | """.stripMargin
    val queryStartTime = System.currentTimeMillis()
    val jobRunId = "00ff4o3b5091080r"
    threadLocalFuture.set(startJob(query, jobRunId))

    val validation: REPLResult => Boolean = result => {
      assert(
        result.results.size == 0,
        s"expected result size is 0, but got ${result.results.size}")
      assert(
        result.schemas.size == 0,
        s"expected schema size is 0, but got ${result.schemas.size}")

      assert(result.status == "FAILED", s"expected status is FAILED, but got ${result.status}")
      assert(!result.error.isEmpty, s"we expect error, but got ${result.error}")
      commonAssert(result, jobRunId, query, queryStartTime)
      true
    }
    pollForResultAndAssert(validation, jobRunId)
  }

  test("describe skipping index") {
    flint
      .skippingIndex()
      .onTable(testTable)
      .addPartitions("year")
      .addValueSet("name")
      .addMinMax("age")
      .create()

    val queryStartTime = System.currentTimeMillis()
    val jobRunId = "00ff4o3b5091080s"
    val query = s"DESC SKIPPING INDEX ON $testTable"
    threadLocalFuture.set(startJob(query, jobRunId))

    val validation: REPLResult => Boolean = result => {
      assert(
        result.results.size == 3,
        s"expected result size is 3, but got ${result.results.size}")
      val expectedResult0 =
        "{'indexed_col_name':'year','data_type':'int','skip_type':'PARTITION'}"
      assert(
        result.results(0) == expectedResult0,
        s"expected result size is $expectedResult0, but got ${result.results(0)}")
      val expectedResult1 =
        "{'indexed_col_name':'name','data_type':'string','skip_type':'VALUE_SET'}"
      assert(
        result.results(1) == expectedResult1,
        s"expected result size is $expectedResult1, but got ${result.results(1)}")
      val expectedResult2 = "{'indexed_col_name':'age','data_type':'int','skip_type':'MIN_MAX'}"
      assert(
        result.results(2) == expectedResult2,
        s"expected result size is $expectedResult2, but got ${result.results(2)}")
      assert(
        result.schemas.size == 3,
        s"expected schema size is 3, but got ${result.schemas.size}")
      val expectedZerothSchema = "{'column_name':'indexed_col_name','data_type':'string'}"
      assert(
        result.schemas(0).equals(expectedZerothSchema),
        s"expected 0th field is $expectedZerothSchema, but got ${result.schemas(0)}")
      val expectedFirstSchema = "{'column_name':'data_type','data_type':'string'}"
      assert(
        result.schemas(1).equals(expectedFirstSchema),
        s"expected 1st field is $expectedFirstSchema, but got ${result.schemas(1)}")
      val expectedSecondSchema = "{'column_name':'skip_type','data_type':'string'}"
      assert(
        result.schemas(2).equals(expectedSecondSchema),
        s"expected 2nd field is $expectedSecondSchema, but got ${result.schemas(2)}")

      assert(result.status == "SUCCESS", s"expected status is FAILED, but got ${result.status}")
      assert(result.error.isEmpty, s"we expect error, but got ${result.error}")

      commonAssert(result, jobRunId, query, queryStartTime)
      true
    }
    pollForResultAndAssert(validation, jobRunId)
  }

  def commonAssert(
      result: REPLResult,
      jobRunId: String,
      query: String,
      queryStartTime: Long): Unit = {
    assert(
      result.jobRunId == jobRunId,
      s"expected jobRunId is $jobRunId, but got ${result.jobRunId}")
    assert(
      result.applicationId == appId,
      s"expected applicationId is $appId, but got ${result.applicationId}")
    assert(
      result.dataSourceName == dataSourceName,
      s"expected data source is $dataSourceName, but got ${result.dataSourceName}")
    val actualQueryText = normalizeString(result.queryText)
    val expectedQueryText = normalizeString(query)
    assert(
      actualQueryText == expectedQueryText,
      s"expected query is $expectedQueryText, but got $actualQueryText")
    assert(result.sessionId.isEmpty, s"we don't expect session id, but got ${result.sessionId}")
    assert(
      result.updateTime > queryStartTime,
      s"expect that update time is ${result.updateTime} later than query start time $queryStartTime, but it is not")
    assert(
      result.queryRunTime > 0,
      s"expected query run time is positive, but got ${result.queryRunTime}")
    assert(
      result.queryRunTime < System.currentTimeMillis() - queryStartTime,
      s"expected query run time ${result.queryRunTime} should be less than ${System
          .currentTimeMillis() - queryStartTime}, but it is not")
    assert(result.queryId.isEmpty, s"we don't expect query id, but got ${result.queryId}")
  }

  def pollForResultAndAssert(expected: REPLResult => Boolean, jobId: String): Unit = {
    pollForResultAndAssert(
      osClient,
      expected,
      "jobRunId",
      jobId,
      streamingTimeout.toMillis,
      resultIndex)
  }

  private def setFlintIndexReadOnly(readonly: Boolean): Unit = {
    openSearchClient
      .indices()
      .putSettings(
        new UpdateSettingsRequest(testIndex).settings(
          Map("index.blocks.write" -> readonly).asJava),
        RequestOptions.DEFAULT)
  }

  private def latestLogEntry(latestId: String): Map[String, AnyRef] = {
    val response = openSearchClient
      .get(
        new GetRequest(s".query_execution_request_$dataSourceName", latestId),
        RequestOptions.DEFAULT)

    Option(response.getSourceAsMap).getOrElse(Collections.emptyMap()).asScala.toMap
  }
}
