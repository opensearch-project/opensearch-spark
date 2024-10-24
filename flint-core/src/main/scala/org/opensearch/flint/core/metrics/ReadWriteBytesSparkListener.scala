/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.metrics

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql.SparkSession

/**
 * Collect and emit bytesRead/Written and recordsRead/Written metrics
 */
class ReadWriteBytesSparkListener extends SparkListener with Logging {
  var bytesRead: Long = 0
  var recordsRead: Long = 0
  var bytesWritten: Long = 0
  var recordsWritten: Long = 0

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    val inputMetrics = taskEnd.taskMetrics.inputMetrics
    val outputMetrics = taskEnd.taskMetrics.outputMetrics
    val ids = s"(${taskEnd.taskInfo.taskId}, ${taskEnd.taskInfo.partitionId})"
    logInfo(
      s"${ids} Input: bytesRead=${inputMetrics.bytesRead}, recordsRead=${inputMetrics.recordsRead}")
    logInfo(
      s"${ids} Output: bytesWritten=${outputMetrics.bytesWritten}, recordsWritten=${outputMetrics.recordsWritten}")

    bytesRead += inputMetrics.bytesRead
    recordsRead += inputMetrics.recordsRead
    bytesWritten += outputMetrics.bytesWritten
    recordsWritten += outputMetrics.recordsWritten
  }

  def emitMetrics(): Unit = {
    logInfo(s"Input: totalBytesRead=${bytesRead}, totalRecordsRead=${recordsRead}")
    logInfo(s"Output: totalBytesWritten=${bytesWritten}, totalRecordsWritten=${recordsWritten}")
    MetricsUtil.addHistoricGauge(MetricConstants.INPUT_TOTAL_BYTES_READ, bytesRead)
    MetricsUtil.addHistoricGauge(MetricConstants.INPUT_TOTAL_RECORDS_READ, recordsRead)
    MetricsUtil.addHistoricGauge(MetricConstants.OUTPUT_TOTAL_BYTES_WRITTEN, bytesWritten)
    MetricsUtil.addHistoricGauge(MetricConstants.OUTPUT_TOTAL_RECORDS_WRITTEN, recordsWritten)
  }
}

object ReadWriteBytesSparkListener {
  def withMetrics[T](spark: SparkSession, lambda: () => T): T = {
    val listener = new ReadWriteBytesSparkListener()
    spark.sparkContext.addSparkListener(listener)

    val result = lambda()

    spark.sparkContext.removeSparkListener(listener)
    listener.emitMetrics()

    result
  }
}
