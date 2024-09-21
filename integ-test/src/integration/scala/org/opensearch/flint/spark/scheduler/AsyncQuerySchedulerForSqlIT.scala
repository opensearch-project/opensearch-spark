/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.scheduler

import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}

import org.opensearch.flint.common.scheduler.AsyncQueryScheduler
import org.opensearch.flint.common.scheduler.model.AsyncQuerySchedulerRequest

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

/**
 * An implementation of AsyncQueryScheduler for Spark SQL integration testing.
 *
 * This scheduler uses a dedicated single-threaded ScheduledExecutorService to run asynchronous
 * SQL queries. It's designed to schedule jobs with a short delay for testing purposes, avoiding
 * the use of the global ExecutionContext to prevent resource conflicts in a Spark environment.
 */
class AsyncQuerySchedulerForSqlIT extends AsyncQueryScheduler with AutoCloseable with Logging {

  lazy val spark = SparkSession.builder().getOrCreate()

  private val executorService: ScheduledExecutorService =
    Executors.newSingleThreadScheduledExecutor()

  override def scheduleJob(asyncQuerySchedulerRequest: AsyncQuerySchedulerRequest): Unit = {
    // Schedule the job to run after 100ms
    executorService.schedule(
      new Runnable {
        override def run(): Unit = {
          logInfo(s"scheduleJob starting...${asyncQuerySchedulerRequest.getScheduledQuery}")
          spark.sql(asyncQuerySchedulerRequest.getScheduledQuery)
          logInfo("scheduleJob complete")
        }
      },
      100,
      TimeUnit.MILLISECONDS)

    logInfo("scheduleJob method returned")
  }

  override def updateJob(asyncQuerySchedulerRequest: AsyncQuerySchedulerRequest): Unit = {
    logInfo("updateJob method returned")
  }

  override def unscheduleJob(asyncQuerySchedulerRequest: AsyncQuerySchedulerRequest): Unit = {
    logInfo("unscheduleJob method returned")
  }

  override def removeJob(asyncQuerySchedulerRequest: AsyncQuerySchedulerRequest): Unit = {
    logInfo("removeJob method returned")
  }

  /**
   * Closes the scheduler and releases resources.
   */
  override def close(): Unit = {
    logInfo("Shutting down AsyncQuerySchedulerForSqlIT")
    executorService.shutdown()
    try {
      if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
        executorService.shutdownNow()
      }
    } catch {
      case _: InterruptedException =>
        executorService.shutdownNow()
    }
  }
}
