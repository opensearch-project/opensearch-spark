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
import org.opensearch.flint.spark.sql.FlintSparkSqlExtensionsParser._

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.Command
import org.apache.spark.sql.types.StringType

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

      val ignoreIfExists = ctx.EXISTS() != null
      val indexOptions = visitPropertyList(ctx.propertyList())
      indexBuilder
        .options(indexOptions)
        .create(ignoreIfExists)

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

  override def visitShowCoveringIndexStatement(
      ctx: ShowCoveringIndexStatementContext): Command = {
    val outputSchema = Seq(AttributeReference("index_name", StringType, nullable = false)())

    FlintSparkSqlCommand(outputSchema) { flint =>
      val fullTableName = getFullTableName(flint, ctx.tableName)
      val indexNamePattern = FlintSparkCoveringIndex.getFlintIndexName("*", fullTableName)
      flint
        .describeIndexes(indexNamePattern)
        .collect { case index: FlintSparkCoveringIndex =>
          Row(index.indexName)
        }
    }
  }

  override def visitDescribeCoveringIndexStatement(
      ctx: DescribeCoveringIndexStatementContext): Command = {
    val outputSchema = Seq(
      AttributeReference("indexed_col_name", StringType, nullable = false)(),
      AttributeReference("data_type", StringType, nullable = false)(),
      AttributeReference("index_type", StringType, nullable = false)())

    FlintSparkSqlCommand(outputSchema) { flint =>
      val indexName = getFlintIndexName(flint, ctx.indexName, ctx.tableName)
      flint
        .describeIndex(indexName)
        .map { case index: FlintSparkCoveringIndex =>
          index.indexedColumns.map { case (colName, colType) =>
            Row(colName, colType, "indexed")
          }.toSeq
        }
        .getOrElse(Seq.empty)
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
