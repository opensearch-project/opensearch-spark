/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.sql

import org.antlr.v4.runtime.ParserRuleContext
import org.antlr.v4.runtime.tree.{ParseTree, RuleNode}
import org.opensearch.flint.spark.FlintSpark
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
}
