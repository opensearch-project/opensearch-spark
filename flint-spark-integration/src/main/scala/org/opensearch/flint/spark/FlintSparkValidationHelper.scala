/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import java.io.IOException
import java.util.UUID

import org.apache.hadoop.fs.Path
import org.opensearch.flint.spark.covering.FlintSparkCoveringIndex
import org.opensearch.flint.spark.mv.FlintSparkMaterializedView
import org.opensearch.flint.spark.skipping.FlintSparkSkippingIndex

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.execution.command.DDLUtils
import org.apache.spark.sql.execution.streaming.CheckpointFileManager
import org.apache.spark.sql.flint.{loadTable, parseTableName, qualifyTableName}

/**
 * Flint Spark validation helper.
 */
trait FlintSparkValidationHelper extends Logging {

  /**
   * Determines whether the source table(s) for a given Flint index are supported.
   *
   * @param spark
   *   Spark session
   * @param index
   *   Flint index
   * @return
   *   true if all non Hive, otherwise false
   */
  def isTableProviderSupported(spark: SparkSession, index: FlintSparkIndex): Boolean = {
    // Extract source table name (possibly more than one for MV query)
    val tableNames = index match {
      case skipping: FlintSparkSkippingIndex => Seq(skipping.tableName)
      case covering: FlintSparkCoveringIndex => Seq(covering.tableName)
      case mv: FlintSparkMaterializedView =>
        spark.sessionState.sqlParser
          .parsePlan(mv.query)
          .collect { case relation: UnresolvedRelation =>
            qualifyTableName(spark, relation.tableName)
          }
    }

    // Validate if any source table is not supported (currently Hive only)
    tableNames.exists { tableName =>
      val (catalog, ident) = parseTableName(spark, tableName)
      val table = loadTable(catalog, ident).get

      // TODO: add allowed table provider list
      DDLUtils.isHiveTable(Option(table.properties().get("provider")))
    }
  }

  /**
   * Checks whether a specified checkpoint location is accessible. Accessibility, in this context,
   * means that the folder exists and the current Spark session has the necessary permissions to
   * access it.
   *
   * @param spark
   *   Spark session
   * @param checkpointLocation
   *   checkpoint location
   * @return
   *   true if accessible, otherwise false
   */
  def isCheckpointLocationAccessible(spark: SparkSession, checkpointLocation: String): Boolean = {
    try {
      val checkpointManager =
        CheckpointFileManager.create(
          new Path(checkpointLocation),
          spark.sessionState.newHadoopConf())

      /*
       * Read permission check: The primary intent here is to catch any exceptions
       * during the accessibility check. The actual result is ignored, as Spark can
       * create any necessary sub-folders when the streaming job starts.
       */
      checkpointManager.exists(new Path(checkpointLocation))

      /*
       * Write permission check: Attempt to create a temporary file to verify write access.
       * The temporary file is left in place in case additional delete permissions are required
       * for some file systems.
       */
      val tempFilePath =
        new Path(
          checkpointManager.createCheckpointDirectory(),
          s"${UUID.randomUUID().toString}.tmp")
      val outputStream = checkpointManager.createAtomic(tempFilePath, overwriteIfPossible = true)
      outputStream.close()

      true
    } catch {
      case e: IOException =>
        logWarning(s"Failed to check if checkpoint location $checkpointLocation accessible", e)
        false
    }
  }
}
