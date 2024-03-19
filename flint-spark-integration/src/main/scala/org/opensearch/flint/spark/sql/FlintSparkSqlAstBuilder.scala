/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.sql

import scala.collection.JavaConverters.mapAsJavaMapConverter

import org.antlr.v4.runtime.ParserRuleContext
import org.antlr.v4.runtime.tree.{ParseTree, RuleNode}
import org.opensearch.flint.spark.{FlintSpark, FlintSparkIndexFactory}
import org.opensearch.flint.spark.FlintSpark.UpdateMode._
import org.opensearch.flint.spark.FlintSparkIndexOptions.OptionName._
import org.opensearch.flint.spark.refresh.FlintSparkIndexRefresh.RefreshMode._
import org.opensearch.flint.spark.sql.covering.FlintSparkCoveringIndexAstBuilder
import org.opensearch.flint.spark.sql.index.FlintSparkIndexAstBuilder
import org.opensearch.flint.spark.sql.job.FlintSparkIndexJobAstBuilder
import org.opensearch.flint.spark.sql.mv.FlintSparkMaterializedViewAstBuilder
import org.opensearch.flint.spark.sql.skipping.FlintSparkSkippingIndexAstBuilder

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.trees.CurrentOrigin
import org.apache.spark.sql.flint.qualifyTableName

/**
 * Flint Spark AST builder that builds Spark command for Flint index statement. This class mix-in
 * all other AST builders and provides util methods.
 */
class FlintSparkSqlAstBuilder
    extends FlintSparkSqlExtensionsBaseVisitor[AnyRef]
    with FlintSparkSkippingIndexAstBuilder
    with FlintSparkCoveringIndexAstBuilder
    with FlintSparkMaterializedViewAstBuilder
    with FlintSparkIndexAstBuilder
    with FlintSparkIndexJobAstBuilder
    with SparkSqlAstBuilder {

  override def visit(tree: ParseTree): LogicalPlan = {
    tree.accept(this).asInstanceOf[LogicalPlan]
  }

  override def aggregateResult(aggregate: AnyRef, nextResult: AnyRef): AnyRef =
    if (nextResult != null) nextResult else aggregate
}

object FlintSparkSqlAstBuilder {

  /**
   * Get full table name if catalog or database not specified. The reason we cannot do this in
   * common SparkSqlAstBuilder.visitTableName is that SparkSession is required to qualify table
   * name which is only available at execution time instead of parsing time.
   *
   * @param flint
   *   Flint Spark which has access to Spark Catalog
   * @param tableNameCtx
   *   table name
   * @return
   */
  def getFullTableName(flint: FlintSpark, tableNameCtx: RuleNode): String = {
    qualifyTableName(flint.spark, tableNameCtx.getText)
  }

  /**
   * Get original SQL text from the origin.
   *
   * @param ctx
   *   rule context to get SQL text associated with
   * @return
   *   SQL text
   */
  def getSqlText(ctx: ParserRuleContext): String = {
    // Origin must be preserved at the beginning of parsing
    val sqlText = CurrentOrigin.get.sqlText.get
    val startIndex = ctx.getStart.getStartIndex
    val stopIndex = ctx.getStop.getStopIndex
    sqlText.substring(startIndex, stopIndex + 1)
  }

  /**
   * Update Flint index metadata and job associated with index.
   *
   * @param flint
   *   Flint Spark which has access to Spark Catalog
   * @param indexName
   *   index name
   * @param updateOptions
   *   options to update
   */
  def updateIndex(flint: FlintSpark, indexName: String, updateOptions: Map[String, String]): Option[String] = {
    val oldIndex = flint.describeIndex(indexName)
      .getOrElse(throw new IllegalStateException(s"Index $indexName doesn't exist"))

    val oldOptions = oldIndex.options.options
    validateOptions(oldOptions, updateOptions)

    val mergedOptions = oldOptions ++ updateOptions
    val newMetadata = oldIndex.metadata().copy(options = mergedOptions.mapValues(_.asInstanceOf[AnyRef]).asJava)
    val newIndex = FlintSparkIndexFactory.create(newMetadata)

    val updateMode = newIndex.options.autoRefresh() match {
      case true => MANUAL_TO_AUTO
      case false => AUTO_TO_MANUAL
    }

    flint.updateIndex(newIndex, updateMode)
  }

  /**
   * Validate update options.
   *
   * @param oldOptions
   *   existing options
   * @param updateOptions
   *   options to update
   */
  private def validateOptions(oldOptions: Map[String, String], updateOptions: Map[String, String]): Unit = {
    val mergedOptions = oldOptions ++ updateOptions
    val newAutoRefresh = mergedOptions.getOrElse(AUTO_REFRESH.toString, "false")
    val oldAutoRefresh = oldOptions.getOrElse(AUTO_REFRESH.toString, "false")

    // auto_refresh must change
    if (newAutoRefresh == oldAutoRefresh) {
      throw new IllegalArgumentException("auto_refresh option must be updated")
    }

    // validate allowed options depending on refresh mode
    val newIncrementalRefresh = mergedOptions.getOrElse(INCREMENTAL_REFRESH.toString, "false")
    val (refreshMode, allowedOptions) = (newAutoRefresh, newIncrementalRefresh) match {
      case ("true", "false") =>
        (AUTO, Set(AUTO_REFRESH, INCREMENTAL_REFRESH, CHECKPOINT_LOCATION))
      case ("false", "false") =>
        (FULL, Set(AUTO_REFRESH, INCREMENTAL_REFRESH))
      case ("false", "true") =>
        (INCREMENTAL, Set(AUTO_REFRESH, INCREMENTAL_REFRESH, WATERMARK_DELAY, CHECKPOINT_LOCATION))
      case ("true", "true") =>
        throw new IllegalArgumentException("auto_refresh and incremental_refresh options cannot both be true")
    }
    if (!updateOptions.keys.forall(allowedOptions.map(_.toString).contains)) {
      throw new IllegalArgumentException(s"Altering to ${refreshMode} refresh index only allows options: ${allowedOptions}")
    }
  }
}
