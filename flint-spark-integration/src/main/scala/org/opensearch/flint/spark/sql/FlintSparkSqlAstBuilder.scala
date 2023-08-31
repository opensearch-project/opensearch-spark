/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.sql

import org.antlr.v4.runtime.tree.RuleNode
import org.opensearch.flint.spark.FlintSpark
import org.opensearch.flint.spark.sql.FlintSparkSqlExtensionsParser.PropertyListContext
import org.opensearch.flint.spark.sql.covering.FlintSparkCoveringIndexAstBuilder
import org.opensearch.flint.spark.sql.skipping.FlintSparkSkippingIndexAstBuilder

import org.apache.spark.sql.catalyst.plans.logical.Command

/**
 * Flint Spark AST builder that builds Spark command for Flint index statement.
 * This class mix-in all other AST builders and provides util methods.
 */
class FlintSparkSqlAstBuilder
    extends FlintSparkSqlExtensionsBaseVisitor[Command]
    with FlintSparkSkippingIndexAstBuilder
    with FlintSparkCoveringIndexAstBuilder {

  override def aggregateResult(aggregate: Command, nextResult: Command): Command =
    if (nextResult != null) nextResult else aggregate
}

object FlintSparkSqlAstBuilder {

  /**
   * Check if auto_refresh is true in property list.
   *
   * @param ctx
   *   property list
   */
  def isAutoRefreshEnabled(ctx: PropertyListContext): Boolean = {
    if (ctx == null) {
      false
    } else {
      ctx
        .property()
        .forEach(p => {
          if (p.key.getText == "auto_refresh") {
            return p.value.getText.toBoolean
          }
        })
      false
    }
  }

  /**
   * Get full table name if database not specified.
   *
   * @param flint
   *   Flint Spark which has access to Spark Catalog
   * @param tableNameCtx
   *   table name
   * @return
   */
  def getFullTableName(flint: FlintSpark, tableNameCtx: RuleNode): String = {
    val tableName = tableNameCtx.getText
    if (tableName.contains(".")) {
      tableName
    } else {
      val db = flint.spark.catalog.currentDatabase
      s"$db.$tableName"
    }
  }
}
