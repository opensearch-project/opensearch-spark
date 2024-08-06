/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import java.util.Base64
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters.mapAsJavaMapConverter

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{doAnswer, spy}
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequest
import org.opensearch.client.RequestOptions
import org.opensearch.flint.OpenSearchTransactionSuite
import org.opensearch.flint.spark.skipping.FlintSparkSkippingIndex.getSkippingIndexName
import org.scalatest.matchers.should.Matchers

import org.apache.spark.sql.flint.config.FlintSparkConf.MONITOR_MAX_ERROR_COUNT
import org.apache.spark.sql.flint.newDaemonThreadPoolScheduledExecutor

class FlintSparkIndexMonitorITSuite extends OpenSearchTransactionSuite with Matchers {

  /** Test table and index name */
  private val testTable = "spark_catalog.default.flint_index_monitor_test"
  private val testFlintIndex = getSkippingIndexName(testTable)
  private val testLatestId: String = Base64.getEncoder.encodeToString(testFlintIndex.getBytes)

  override def beforeAll(): Unit = {
    super.beforeAll()
    createPartitionedAddressTable(testTable)

    // Replace mock executor with real one and change its delay
    val realExecutor = newDaemonThreadPoolScheduledExecutor("flint-index-heartbeat", 1)
    FlintSparkIndexMonitor.executor = spy(realExecutor)
    doAnswer(invocation => {
      // Delay 5 seconds to wait for refresh index done
      realExecutor.scheduleWithFixedDelay(invocation.getArgument(0), 5, 1, TimeUnit.SECONDS)
    }).when(FlintSparkIndexMonitor.executor)
      .scheduleWithFixedDelay(any[Runnable], any[Long], any[Long], any[TimeUnit])

    // Set max error count higher to avoid impact on transient error test case
    setFlintSparkConf(MONITOR_MAX_ERROR_COUNT, 10)
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    flint
      .skippingIndex()
      .onTable(testTable)
      .addValueSet("name")
      .options(FlintSparkIndexOptions(Map("auto_refresh" -> "true")))
      .create()
    flint.refreshIndex(testFlintIndex)

    // Wait for refresh complete and another 5 seconds to make sure monitor thread start
    val jobId = spark.streams.active.find(_.name == testFlintIndex).get.id.toString
    awaitStreamingComplete(jobId)
    Thread.sleep(5000L)
  }

  override def afterEach(): Unit = {
    // Cancel task to avoid conflict with delete operation since it runs frequently
    FlintSparkIndexMonitor.indexMonitorTracker.values.foreach(_.cancel(true))
    FlintSparkIndexMonitor.indexMonitorTracker.clear()

    try {
      deleteTestIndex(testFlintIndex)
    } finally {
      super.afterEach()
    }
  }

  test("job start time should not change and last update time should keep updated") {
    var (prevJobStartTime, prevLastUpdateTime) = getLatestTimestamp
    3 times { (jobStartTime, lastUpdateTime) =>
      jobStartTime shouldBe prevJobStartTime
      lastUpdateTime should be > prevLastUpdateTime
      prevLastUpdateTime = lastUpdateTime
    }
  }

  test("job start time should not change until recover index") {
    val (prevJobStartTime, _) = getLatestTimestamp

    // Stop streaming job and wait for monitor task stopped
    spark.streams.active.find(_.name == testFlintIndex).get.stop()
    waitForMonitorTaskRun()

    // Restart streaming job and monitor task
    flint.recoverIndex(testFlintIndex)
    waitForMonitorTaskRun()

    val (jobStartTime, _) = getLatestTimestamp
    jobStartTime should be > prevJobStartTime
  }

  test("monitor task should terminate if streaming job inactive") {
    val task = FlintSparkIndexMonitor.indexMonitorTracker(testFlintIndex)

    // Stop streaming job and wait for monitor task stopped
    spark.streams.active.find(_.name == testFlintIndex).get.stop()
    waitForMonitorTaskRun()

    // Monitor task should be cancelled
    task.isCancelled shouldBe true
  }

  test("monitor task should not terminate if any exception") {
    // Block write on metadata log index
    setWriteBlockOnMetadataLogIndex(true)
    waitForMonitorTaskRun()

    // Monitor task should stop working after blocking writes
    var (_, prevLastUpdateTime) = getLatestTimestamp
    1 times { (_, lastUpdateTime) =>
      lastUpdateTime shouldBe prevLastUpdateTime
    }

    // Unblock write and wait for monitor task attempt to update again
    setWriteBlockOnMetadataLogIndex(false)
    waitForMonitorTaskRun()

    // Monitor task continue working after unblocking write
    3 times { (_, lastUpdateTime) =>
      lastUpdateTime should be > prevLastUpdateTime
      prevLastUpdateTime = lastUpdateTime
    }
  }

  test("monitor task and streaming job should terminate if exception occurred consistently") {
    val task = FlintSparkIndexMonitor.indexMonitorTracker(testFlintIndex)

    // Block write on metadata log index
    setWriteBlockOnMetadataLogIndex(true)
    waitForMonitorTaskRun()

    // Both monitor task and streaming job should stop after 10 times
    10 times { (_, _) =>
      {
        // assert nothing. just wait enough times of task execution
      }
    }

    task.isCancelled shouldBe true
    spark.streams.active.exists(_.name == testFlintIndex) shouldBe false
  }

  test("await monitor terminated without exception should stay refreshing state") {
    // Setup a timer to terminate the streaming job
    new Thread(() => {
      Thread.sleep(3000L)
      spark.streams.active.find(_.name == testFlintIndex).get.stop()
    }).start()

    // Await until streaming job terminated
    flint.flintIndexMonitor.awaitMonitor()

    // Assert index state is active now
    val latestLog = latestLogEntry(testLatestId)
    latestLog should contain("state" -> "refreshing")
  }

  test("await monitor terminated with exception should update index state to failed with error") {
    new Thread(() => {
      Thread.sleep(3000L)

      // Set Flint index readonly to simulate streaming job exception
      val settings = Map("index.blocks.write" -> true)
      val request = new UpdateSettingsRequest(testFlintIndex).settings(settings.asJava)
      openSearchClient.indices().putSettings(request, RequestOptions.DEFAULT)

      // Trigger a new micro batch execution
      sql(s"""
           | INSERT INTO $testTable
           | PARTITION (year=2023, month=6)
           | VALUES ('Test', 35, 'Vancouver')
           | """.stripMargin)
    }).start()

    // Await until streaming job terminated
    flint.flintIndexMonitor.awaitMonitor()

    // Assert index state is active now
    val latestLog = latestLogEntry(testLatestId)
    latestLog should contain("state" -> "failed")
    latestLog("error").asInstanceOf[String] should (include("OpenSearchException") and
      include("type=cluster_block_exception"))
  }

  test(
    "await monitor terminated with streaming job exit early should update index state to failed") {
    // Terminate streaming job intentionally before await
    spark.streams.active.find(_.name == testFlintIndex).get.stop()

    // Await until streaming job terminated
    flint.flintIndexMonitor.awaitMonitor()

    // Assert index state is active now
    val latestLog = latestLogEntry(testLatestId)
    latestLog should contain("state" -> "failed")
    latestLog should not contain "error"
  }

  private def getLatestTimestamp: (Long, Long) = {
    val latest = latestLogEntry(testLatestId)
    (latest("jobStartTime").asInstanceOf[Long], latest("lastUpdateTime").asInstanceOf[Long])
  }

  private implicit class intWithTimes(n: Int) {
    def times(f: (Long, Long) => Unit): Unit = {
      1 to n foreach { _ =>
        {
          waitForMonitorTaskRun()

          val (jobStartTime, lastUpdateTime) = getLatestTimestamp
          f(jobStartTime, lastUpdateTime)
        }
      }
    }
  }

  private def waitForMonitorTaskRun(): Unit = {
    // Interval longer than monitor schedule to make sure it has finished another run
    Thread.sleep(3000L)
  }

  private def setWriteBlockOnMetadataLogIndex(isBlock: Boolean): Unit = {
    val request = new UpdateSettingsRequest(testMetaLogIndex)
      .settings(Map("blocks.write" -> isBlock).asJava) // Blocking write operations
    openSearchClient.indices().putSettings(request, RequestOptions.DEFAULT)
  }
}
