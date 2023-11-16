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
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequest
import org.opensearch.client.RequestOptions
import org.opensearch.flint.OpenSearchTransactionSuite
import org.opensearch.flint.spark.FlintSpark.RefreshMode._
import org.opensearch.flint.spark.skipping.FlintSparkSkippingIndex.getSkippingIndexName
import org.scalatest.matchers.should.Matchers

import org.apache.spark.sql.flint.newDaemonThreadPoolScheduledExecutor

class FlintSparkIndexMonitorITSuite extends OpenSearchTransactionSuite with Matchers {

  /** Test table and index name */
  private val testTable = "spark_catalog.default.flint_index_monitor_test"
  private val testFlintIndex = getSkippingIndexName(testTable)
  private val testLatestId: String = Base64.getEncoder.encodeToString(testFlintIndex.getBytes)

  override def beforeAll(): Unit = {
    super.beforeAll()
    createPartitionedTable(testTable)

    // Replace mock executor with real one and change its delay
    val realExecutor = newDaemonThreadPoolScheduledExecutor("flint-index-heartbeat", 1)
    FlintSparkIndexMonitor.executor = spy(realExecutor)
    doAnswer(invocation => {
      // Delay 5 seconds to wait for refresh index done
      realExecutor.scheduleWithFixedDelay(invocation.getArgument(0), 5, 1, TimeUnit.SECONDS)
    }).when(FlintSparkIndexMonitor.executor)
      .scheduleWithFixedDelay(any[Runnable], any[Long], any[Long], any[TimeUnit])
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    flint
      .skippingIndex()
      .onTable(testTable)
      .addValueSet("name")
      .options(FlintSparkIndexOptions(Map("auto_refresh" -> "true")))
      .create()
    flint.refreshIndex(testFlintIndex, INCREMENTAL)

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
      flint.deleteIndex(testFlintIndex)
    } catch {
      // Index maybe end up with failed state in some test
      case _: IllegalStateException =>
        openSearchClient
          .indices()
          .delete(new DeleteIndexRequest(testFlintIndex), RequestOptions.DEFAULT)
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

  test("monitor task should terminate if streaming job inactive") {
    val task = FlintSparkIndexMonitor.indexMonitorTracker(testFlintIndex)

    // Stop streaming job intentionally
    spark.streams.active.find(_.name == testFlintIndex).get.stop()
    waitForMonitorTaskRun()

    // Index state transit to failed and task is cancelled
    latestLogEntry(testLatestId) should contain("state" -> "failed")
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
