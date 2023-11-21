/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.util

import java.util.concurrent.{ScheduledExecutorService, TimeUnit}

import scala.collection.JavaConverters._

import org.apache.spark.internal.Logging

trait ThreadPoolFactory extends Logging {
  def newDaemonThreadPoolScheduledExecutor(
      threadNamePrefix: String,
      numThreads: Int): ScheduledExecutorService

  def shutdownThreadPool(executor: ScheduledExecutorService): Unit = {
    logInfo(s"terminate executor ${executor}")
    executor.shutdown() // Disable new tasks from being submitted

    try {
      // Wait a while for existing tasks to terminate
      if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
        logWarning("Executor did not terminate in the specified time.")
        val tasksNotExecuted = executor.shutdownNow() // Cancel currently executing tasks
        // Log the tasks that were awaiting execution
        logInfo(s"The following tasks were cancelled: $tasksNotExecuted")

        // Wait a while for tasks to respond to being cancelled
        if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
          logError("Thread pool did not terminate after shutdownNow.")
        }
      }
    } catch {
      case ie: InterruptedException =>
        // (Re-)Cancel if current thread also interrupted
        executor.shutdownNow()
        // Log the interrupted status
        logError("Shutdown interrupted", ie)
        // Preserve interrupt status
        Thread.currentThread().interrupt()
    }
  }

  /**
   * Checks if there are any non-daemon threads other than the "main" thread.
   *
   * @return
   *   true if non-daemon threads other than "main" are active, false otherwise.
   */
  def hasNonDaemonThreadsOtherThanMain(): Boolean = {
    // Log thread information and check for non-daemon threads
    Thread.getAllStackTraces.keySet.asScala.exists { t =>
      val thread = t.asInstanceOf[Thread]
      val isNonDaemon = !thread.isDaemon && thread.getName != "main"

      // Log the thread information
      logInfo(s"Name: ${thread.getName}; IsDaemon: ${thread.isDaemon}; State: ${thread.getState}")

      // Log the stack trace
      Option(Thread.getAllStackTraces.get(thread)).foreach(_.foreach(traceElement =>
        logInfo(s"    at $traceElement")))

      // Return true if a non-daemon thread is found
      isNonDaemon
    }
  }
}
