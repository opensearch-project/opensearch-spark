/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.util

import org.apache.spark.{MapOutputTrackerMaster, SparkEnv}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQueryListener

/**
 * Clean Spark shuffle data after each microBatch.
 * https://github.com/opensearch-project/opensearch-spark/issues/302
 */
class ShuffleCleaner(spark: SparkSession) extends StreamingQueryListener with Logging {

  override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {}

  override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
    ShuffleCleaner.cleanUp(spark)
  }

  override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {}
}

trait Cleaner {
  def cleanUp(spark: SparkSession)
}

object CleanerFactory {
  def cleaner(streaming: Boolean): Cleaner = {
    if (streaming) NoOpCleaner else ShuffleCleaner
  }
}

/**
 * No operation cleaner.
 */
object NoOpCleaner extends Cleaner {
  override def cleanUp(spark: SparkSession): Unit = {}
}

/**
 * Spark shuffle data cleaner.
 */
object ShuffleCleaner extends Cleaner with Logging {
  def cleanUp(spark: SparkSession): Unit = {
    logInfo("Before cleanUp Shuffle")
    val cleaner = spark.sparkContext.cleaner
    val masterTracker = SparkEnv.get.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
    val shuffleIds = masterTracker.shuffleStatuses.keys.toSet
    shuffleIds.foreach(shuffleId => cleaner.foreach(c => c.doCleanupShuffle(shuffleId, true)))
    logInfo("After cleanUp Shuffle")
  }
}
