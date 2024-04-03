/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import java.io.IOException

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
trait FlintSparkValidationHelper {
  self: Logging =>

  /**
   * Validate if source table(s) of the given Flint index are not Hive table.
   *
   * @param spark
   *   Spark session
   * @param index
   *   Flint index
   * @return
   *   true if all non Hive, otherwise false
   */
  def isSourceTableHive(spark: SparkSession, index: FlintSparkIndex): Boolean = {
    // Extract source table name (possibly more than 1 for MV source query)
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

    // Validate if any source table is Hive
    tableNames.exists { tableName =>
      val (catalog, ident) = parseTableName(spark, tableName)
      val table = loadTable(catalog, ident).get
      DDLUtils.isHiveTable(Option(table.properties().get("provider")))
    }
  }

  /**
   * Validate if checkpoint location is accessible (the folder exists and current Spark session
   * has permission to access).
   *
   * @param spark
   *   Spark session
   * @param checkpointLocation
   *   checkpoint location
   * @return
   *   true if accessible, otherwise false
   */
  def isCheckpointLocationAccessible(spark: SparkSession, checkpointLocation: String): Boolean = {
    val checkpointPath = new Path(checkpointLocation)
    val checkpointManager =
      CheckpointFileManager.create(checkpointPath, spark.sessionState.newHadoopConf())
    try {
      checkpointManager.exists(checkpointPath)
    } catch {
      case e: IOException =>
        logWarning(s"Failed to check if checkpoint location $checkpointLocation exists", e)
        false
    }
  }
}
