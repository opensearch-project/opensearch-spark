/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.metrics

import org.apache.spark.scheduler.{SparkListenerStageSubmitted, SparkListenerTaskEnd}

case class Progress(
    bytesCompleted: Long,
    estimatedBytesTotal: Double,
    bytesPerSecond: Double,
) {

}

/**
 * Collect and emit metrics by listening spark events
 */
case class ProgressListener() extends FlintListener {
  // For a stage, we keep track of when it started and the tasks we'll need to go through
  private var startTime: Long = 0
  private var numTasks: Long = 0

  // Then for each task, we want to use runtime information to compute the processing rate and estimate remaining bytes.
  // We keep everything as doubles since it reduces the need for type casting.
  private var tasksComplete: Long = 0
  private var bytesProcessed: Long = 0
  private var elapsedSeconds: Double = 0.0

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    this.startTime = System.currentTimeMillis()
    this.numTasks = stageSubmitted.stageInfo.numTasks
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    this.tasksComplete += 1
    this.bytesProcessed += taskEnd.taskMetrics.inputMetrics.bytesRead
    this.elapsedSeconds = 0.001 * (System.currentTimeMillis() - this.startTime)
  }

  def currentProgress(): Progress = Progress(
    this.bytesProcessed,
    // Lower-clamp divisors to avoid division by 0. In these cases it's fine if the results aren't accurate,
    // it should stabilize quickly as more tasks are completed.
    this.numTasks * (this.bytesProcessed.asInstanceOf[Double] / Math.max(1.0, this.tasksComplete)),
    this.bytesProcessed / Math.max(1.0, this.elapsedSeconds)
  )

  def finish(): Unit = {
    // No-op
  }
}
