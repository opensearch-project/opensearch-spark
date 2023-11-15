/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import java.util.Base64
import java.util.concurrent.TimeUnit

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{doAnswer, spy}
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

  test("test") {
    flint
      .skippingIndex()
      .onTable(testTable)
      .addValueSet("name")
      .options(FlintSparkIndexOptions(Map("auto_refresh" -> "true")))
      .create()
    flint.refreshIndex(testFlintIndex, INCREMENTAL)

    // Wait for refresh complete and monitor thread start
    val jobId = spark.streams.active.find(_.name == testFlintIndex).get.id.toString
    awaitStreamingComplete(jobId)
    Thread.sleep(5000L)

    var (prevJobStartTime, prevLastUpdateTime) = getLatestTimestamp

    // jobStartTime should stay same while lastUpdateTime keep updated
    3 times { (jobStartTime, lastUpdateTime) =>
      jobStartTime shouldBe prevJobStartTime
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
          // Sleep longer than monitor interval 1 second
          Thread.sleep(3000)

          val (jobStartTime, lastUpdateTime) = getLatestTimestamp
          f(jobStartTime, lastUpdateTime)
        }
      }
    }
  }
}
