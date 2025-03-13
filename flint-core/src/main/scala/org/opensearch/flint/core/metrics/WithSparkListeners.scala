/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.metrics

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.SparkListener
import org.apache.spark.sql.SparkSession

/**
 * A FlintListener represents a SparkListener with a finalization operation to process collected
 * events.
 */
trait FlintListener extends SparkListener with Logging {

  /**
   * Perform any finalizing operations with the listener's collected metrics, such as emitting
   * them to an external Sink or assembling a data structure for the caller.
   */
  def complete(): Unit
}

/**
 * Construct a context for running jobs which includes all provided listeners.
 *
 * @param spark
 *   A spark session to run jobs in.
 * @param listeners
 *   A list of listeners that will be attached to the job and cleaned + finalized after the job is
 *   run.
 */
case class WithSparkListeners(spark: SparkSession, listeners: List[FlintListener])
    extends Logging {
  private def complete(listener: FlintListener): Unit = {
    try {
      listener.complete()
    } catch {
      case ex: Exception =>
        // Job listeners shouldn't interfere with the actual job behavior, so we ignore any related issues.
        logWarning(
          s"The listener $listener threw an exception. " +
            s"Listener output may be incomplete or missing. Cause: $ex.")
    }
  }

  def run[T](lambda: () => T): T = {
    for (listener <- listeners) {
      spark.sparkContext.addSparkListener(listener)
    }

    val result = lambda()

    for (listener <- listeners) {
      spark.sparkContext.removeSparkListener(listener)
      complete(listener)
    }

    result
  }
}
