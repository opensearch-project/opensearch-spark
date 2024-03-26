/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.refresh

import java.io.IOException

import org.apache.hadoop.fs.Path
import org.opensearch.flint.spark.FlintSparkIndex
import org.opensearch.flint.spark.covering.FlintSparkCoveringIndex
import org.opensearch.flint.spark.mv.FlintSparkMaterializedView
import org.opensearch.flint.spark.refresh.FlintSparkIndexRefresh.RefreshMode.RefreshMode
import org.opensearch.flint.spark.skipping.FlintSparkSkippingIndex

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.command.DDLUtils
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.streaming.CheckpointFileManager
import org.apache.spark.sql.flint.{loadTable, parseTableName, qualifyTableName}
import org.apache.spark.sql.flint.config.FlintSparkConf

/**
 * Flint Spark index refresh that sync index data with source in style defined by concrete
 * implementation class.
 */
trait FlintSparkIndexRefresh extends Logging {

  /**
   * @return
   *   refresh mode
   */
  def refreshMode: RefreshMode

  /**
   * Validate the current index refresh beforehand.
   */
  def validate(spark: SparkSession): Unit = {}

  /**
   * Start refreshing the index.
   *
   * @param spark
   *   Spark session to submit job
   * @param flintSparkConf
   *   Flint Spark configuration
   * @return
   *   optional Spark job ID
   */
  def start(spark: SparkSession, flintSparkConf: FlintSparkConf): Option[String]
}

object FlintSparkIndexRefresh extends Logging {

  /** Index refresh mode */
  object RefreshMode extends Enumeration {
    type RefreshMode = Value
    val AUTO, FULL, INCREMENTAL = Value
  }

  /**
   * Create concrete index refresh implementation for the given index.
   *
   * @param indexName
   *   Flint index name
   * @param index
   *   Flint index
   * @return
   *   index refresh
   */
  def create(indexName: String, index: FlintSparkIndex): FlintSparkIndexRefresh = {
    val options = index.options
    if (options.autoRefresh()) {
      new AutoIndexRefresh(indexName, index)
    } else if (options.incrementalRefresh()) {
      new IncrementalIndexRefresh(indexName, index)
    } else {
      new FullIndexRefresh(indexName, index)
    }
  }

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
  def isSourceTableNonHive(spark: SparkSession, index: FlintSparkIndex): Boolean = {
    // Extract source table name (possibly more than 1 for MV source query)
    val tableNames = index match {
      case skipping: FlintSparkSkippingIndex => Seq(skipping.tableName)
      case covering: FlintSparkCoveringIndex => Seq(covering.tableName)
      case mv: FlintSparkMaterializedView =>
        spark.sessionState.sqlParser
          .parsePlan(mv.query)
          .collect { case LogicalRelation(_, _, Some(table), _) =>
            qualifyTableName(spark, table.identifier.table)
          }
    }

    // Validate each source table is Hive
    tableNames.forall { tableName =>
      val (catalog, ident) = parseTableName(spark, tableName)
      val table = loadTable(catalog, ident).get
      !DDLUtils.isHiveTable(Option(table.properties().get("provider")))
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
      // require(
      checkpointManager.exists(checkpointPath)
      //  s"Checkpoint location $checkpointLocation doesn't exist")
    } catch {
      case e: IOException =>
        logWarning(s"Failed to check if checkpoint location $checkpointLocation exists", e)
        // throw new IllegalArgumentException(
        //  s"No permission to access checkpoint location $checkpointLocation")
        false
    }
  }
}
