/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.sql.covering

import org.antlr.v4.runtime.tree.RuleNode
import org.opensearch.flint.spark.FlintSpark
import org.opensearch.flint.spark.FlintSpark.RefreshMode
import org.opensearch.flint.spark.covering.FlintSparkCoveringIndex
import org.opensearch.flint.spark.sql.{FlintSparkSqlCommand, FlintSparkSqlExtensionsVisitor, SparkSqlAstBuilder}
import org.opensearch.flint.spark.sql.FlintSparkSqlAstBuilder.getFullTableName
import org.opensearch.flint.spark.sql.FlintSparkSqlExtensionsParser.{CreateCoveringIndexStatementContext, DropCoveringIndexStatementContext, RefreshCoveringIndexStatementContext}

import org.apache.spark.sql.catalyst.plans.logical.Command

/**
 * Flint Spark AST builder that builds Spark command for Flint covering index statement.
 */
trait FlintSparkCoveringIndexAstBuilder extends FlintSparkSqlExtensionsVisitor[AnyRef] {
  self: SparkSqlAstBuilder =>

  override def visitCreateCoveringIndexStatement(
      ctx: CreateCoveringIndexStatementContext): Command = {
    FlintSparkSqlCommand() { flint =>
      val indexName = ctx.indexName.getText
      val tableName = ctx.tableName.getText
      val indexBuilder =
        flint
          .coveringIndex()
          .name(indexName)
          .onTable(tableName)

      ctx.indexColumns.multipartIdentifierProperty().forEach { indexColCtx =>
        val colName = indexColCtx.multipartIdentifier().getText
        indexBuilder.addIndexColumns(colName)
      }

      val indexOptions = visitPropertyList(ctx.propertyList())
      indexBuilder
        .options(indexOptions)
        .create()

      // Trigger auto refresh if enabled
      if (indexOptions.autoRefresh()) {
        val flintIndexName = getFlintIndexName(flint, ctx.indexName, ctx.tableName)
        flint.refreshIndex(flintIndexName, RefreshMode.INCREMENTAL)
      }
      Seq.empty
    }
  }

  override def visitRefreshCoveringIndexStatement(
      ctx: RefreshCoveringIndexStatementContext): Command = {
    FlintSparkSqlCommand() { flint =>
      val flintIndexName = getFlintIndexName(flint, ctx.indexName, ctx.tableName)
      flint.refreshIndex(flintIndexName, RefreshMode.FULL)
      Seq.empty
    }
  }

  override def visitDropCoveringIndexStatement(
      ctx: DropCoveringIndexStatementContext): Command = {
    FlintSparkSqlCommand() { flint =>
      val flintIndexName = getFlintIndexName(flint, ctx.indexName, ctx.tableName)
      flint.deleteIndex(flintIndexName)
      Seq.empty
    }
  }

  private def getFlintIndexName(
      flint: FlintSpark,
      indexNameCtx: RuleNode,
      tableNameCtx: RuleNode): String = {
    val indexName = indexNameCtx.getText
    val fullTableName = getFullTableName(flint, tableNameCtx)
    FlintSparkCoveringIndex.getFlintIndexName(indexName, fullTableName)
  }
}
