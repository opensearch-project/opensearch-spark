/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.scheduler

import java.time.Instant
import java.util.Locale

import scala.jdk.CollectionConverters.mapAsJavaMapConverter

import org.opensearch.action.admin.indices.delete.DeleteIndexRequest
import org.opensearch.action.get.GetRequest
import org.opensearch.client.RequestOptions
import org.opensearch.client.indices.GetIndexRequest
import org.opensearch.flint.common.scheduler.model.AsyncQuerySchedulerRequest
import org.opensearch.flint.core.FlintOptions
import org.opensearch.flint.core.storage.OpenSearchClientUtils
import org.opensearch.flint.spark.FlintSparkSuite
import org.opensearch.flint.spark.scheduler.OpenSearchAsyncQueryScheduler
import org.scalatest.matchers.should.Matchers

class OpenSearchAsyncQuerySchedulerITSuite extends FlintSparkSuite with Matchers {

  private var scheduler: OpenSearchAsyncQueryScheduler = _
  private val testJobId = "test_job"

  override def beforeAll(): Unit = {
    super.beforeAll()
    val flintOptions = new FlintOptions(openSearchOptions.asJava)
    scheduler = new OpenSearchAsyncQueryScheduler(flintOptions)
  }

  override def afterEach(): Unit = {
    super.afterEach()
    deleteSchedulerIndex()
  }

  test("schedule, update, unschedule, and remove job") {
    val request = createTestRequest()

    // Schedule job
    scheduler.scheduleJob(request)
    val (exists, query, enabled, schedule) = verifyJob(testJobId)
    exists shouldBe true
    query shouldBe "SELECT * FROM test_table"
    enabled shouldBe true
    schedule shouldBe "1 minutes"

    // Update job with new query and schedule
    val updatedRequest =
      AsyncQuerySchedulerRequest.builder().jobId(testJobId).interval("5 minutes").build()
    scheduler.updateJob(updatedRequest)
    val (_, _, _, updatedSchedule) = verifyJob(testJobId)
    updatedSchedule shouldBe "5 minutes"

    // Unschedule job
    scheduler.unscheduleJob(request)
    val (_, _, unscheduledEnabled, _) = verifyJob(testJobId)
    unscheduledEnabled shouldBe false

    // Remove job
    scheduler.removeJob(request)
    val (removedExists, _, _, _) = verifyJob(testJobId)
    removedExists shouldBe false
  }

  test("schedule job conflict") {
    val request = createTestRequest()
    scheduler.scheduleJob(request)

    val conflictRequest =
      AsyncQuerySchedulerRequest
        .builder()
        .jobId(testJobId)
        .interval("2 minutes")
        .lastUpdateTime(Instant.now())
        .build()
    assertThrows[IllegalArgumentException] {
      scheduler.scheduleJob(conflictRequest)
    }
  }

  test("unschedule non-existent job") {
    val request = createTestRequest()
    scheduler.scheduleJob(request)

    val unscheduleJobRequest =
      AsyncQuerySchedulerRequest.builder().jobId("non_existent_job").build()
    assertThrows[IllegalArgumentException] {
      scheduler.unscheduleJob(unscheduleJobRequest)
    }
  }

  test("update non-existent job") {
    val request = createTestRequest()
    scheduler.updateJob(request)

    val (exists, query, enabled, schedule) = verifyJob(testJobId)
    exists shouldBe true
    query shouldBe "SELECT * FROM test_table"
    enabled shouldBe true
    schedule shouldBe "1 minutes"
  }

  test("remove non-existent job") {
    val request = createTestRequest()
    scheduler.scheduleJob(request)

    val removeRequest = AsyncQuerySchedulerRequest.builder().jobId("non_existent_job").build()
    assertThrows[IllegalArgumentException] {
      scheduler.removeJob(removeRequest)
    }
  }

  private def createTestRequest(): AsyncQuerySchedulerRequest = {
    AsyncQuerySchedulerRequest
      .builder()
      .accountId("test_account")
      .jobId(testJobId)
      .dataSource("test_datasource")
      .scheduledQuery("SELECT * FROM test_table")
      .queryLang("SQL")
      .interval("1 minutes")
      .enabled(true)
      .lastUpdateTime(Instant.now())
      .build()
  }

  private def verifyJob(jobId: String): (Boolean, String, Boolean, String) = {
    val client = OpenSearchClientUtils.createClient(new FlintOptions(openSearchOptions.asJava))
    val response = client.get(
      new GetRequest(OpenSearchAsyncQueryScheduler.SCHEDULER_INDEX_NAME, jobId),
      RequestOptions.DEFAULT)

    if (response.isExists) {
      val sourceMap = response.getSourceAsMap
      val exists = true
      val query = sourceMap.get("scheduledQuery").asInstanceOf[String]
      val enabled = sourceMap.get("enabled").asInstanceOf[Boolean]
      val scheduleMap = sourceMap.get("schedule").asInstanceOf[java.util.Map[String, Any]]
      val intervalMap = scheduleMap.get("interval").asInstanceOf[java.util.Map[String, Any]]
      val schedule =
        s"${intervalMap.get("period")} ${intervalMap.get("unit").toString.toLowerCase(Locale.ROOT)}"

      (exists, query, enabled, schedule)
    } else {
      (false, "", false, "")
    }
  }

  private def deleteSchedulerIndex(): Unit = {
    val client = OpenSearchClientUtils.createClient(new FlintOptions(openSearchOptions.asJava))
    val deleteRequest = new DeleteIndexRequest(OpenSearchAsyncQueryScheduler.SCHEDULER_INDEX_NAME)
    try {
      client.deleteIndex(deleteRequest, RequestOptions.DEFAULT)
    } catch {
      case _: Exception => // Index may not exist, ignore
    }
  }
}
