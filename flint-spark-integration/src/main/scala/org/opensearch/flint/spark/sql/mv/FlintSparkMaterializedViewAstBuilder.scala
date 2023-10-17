/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.sql.mv

import org.opensearch.flint.spark.sql.FlintSparkSqlExtensionsParser.{CreateMaterializedViewStatementContext, DropMaterializedViewStatementContext, MaterializedViewQueryContext}
import org.opensearch.flint.spark.sql.FlintSparkSqlExtensionsVisitor

import org.apache.spark.sql.catalyst.trees.CurrentOrigin

/**
 * Flint Spark AST builder that builds Spark command for Flint materialized view statement.
 */
trait FlintSparkMaterializedViewAstBuilder extends FlintSparkSqlExtensionsVisitor[AnyRef] {

  override def visitCreateMaterializedViewStatement(
      ctx: CreateMaterializedViewStatementContext): AnyRef = {
    val mvName = ctx.mvName.getText
    val query = getMvQuery(ctx.query)
    throw new UnsupportedOperationException(s"Create MV $mvName with query $query")
  }

  override def visitDropMaterializedViewStatement(
      ctx: DropMaterializedViewStatementContext): AnyRef = {
    throw new UnsupportedOperationException(s"Drop MV ${ctx.mvName.getText}")
  }

  private def getMvQuery(ctx: MaterializedViewQueryContext): String = {
    // Assume origin must be preserved at the beginning of parsing
    val sqlText = CurrentOrigin.get.sqlText.get
    val startIndex = ctx.getStart.getStartIndex
    val stopIndex = ctx.getStop.getStopIndex
    sqlText.substring(startIndex, stopIndex + 1)
  }
}
