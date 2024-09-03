/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import java.util.UUID

import org.apache.hadoop.fs.{FSDataOutputStream, Path}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.CheckpointFileManager
import org.apache.spark.sql.execution.streaming.CheckpointFileManager.RenameHelperMethods

/**
 * Manages the checkpoint directory for Flint indexes.
 *
 * @param spark
 *   The SparkSession used for Hadoop configuration.
 * @param checkpointLocation
 *   The path to the checkpoint directory.
 */
class FlintSparkCheckpoint(spark: SparkSession, checkpointLocation: String) extends Logging {
  private val checkpointDir = new Path(checkpointLocation)
  private val checkpointManager =
    CheckpointFileManager.create(checkpointDir, spark.sessionState.newHadoopConf())

  /**
   * Checks if the checkpoint directory exists.
   *
   * @return
   *   true if the checkpoint directory exists, false otherwise.
   */
  def exists(): Boolean = checkpointManager.exists(checkpointDir)

  /**
   * Creates a temporary file in the checkpoint directory.
   *
   * @return
   *   An optional FSDataOutputStream for the created temporary file, or None if creation fails.
   */
  def createTempFile(): Option[FSDataOutputStream] = {
    checkpointManager match {
      case manager: RenameHelperMethods =>
        val tempFilePath =
          new Path(
            checkpointManager.createCheckpointDirectory(), // create all parent folders if needed
            s"${UUID.randomUUID().toString}.tmp")
        Some(manager.createTempFile(tempFilePath))
      case _ =>
        logInfo(s"Cannot create temp file at checkpoint location: ${checkpointManager.getClass}")
        None
    }
  }

  /**
   * Deletes the checkpoint directory. This method attempts to delete the checkpoint directory and
   * captures any exceptions that occur. Exceptions are logged but ignored so as not to disrupt
   * the caller's workflow.
   */
  def delete(): Unit = {
    try {
      checkpointManager.delete(checkpointDir)
      logInfo(s"Checkpoint directory $checkpointDir deleted.")
    } catch {
      case e: Exception =>
        logError(s"Error deleting checkpoint directory $checkpointDir", e)
    }
  }
}
