/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.sql.mv

import org.opensearch.flint.spark.sql.FlintSparkSqlExtensionsParser.{CreateMaterializedViewStatementContext, DropMaterializedViewStatementContext}
import org.opensearch.flint.spark.sql.FlintSparkSqlExtensionsVisitor

/**
 * Flint Spark AST builder that builds Spark command for Flint materialized view statement.
 */
trait FlintSparkMaterializedViewAstBuilder extends FlintSparkSqlExtensionsVisitor[AnyRef] {

  override def visitCreateMaterializedViewStatement(
      ctx: CreateMaterializedViewStatementContext): AnyRef = {
    val mvName = ctx.mvName.getText
    val query = ctx.mvQuery.getText
    throw new UnsupportedOperationException(s"Create MV $mvName with query $query")
  }

  override def visitDropMaterializedViewStatement(
      ctx: DropMaterializedViewStatementContext): AnyRef = {
    throw new UnsupportedOperationException(s"Drop MV ${ctx.mvName.getText}")
  }
}
