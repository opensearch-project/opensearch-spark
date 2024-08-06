/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.sql.covering

import org.antlr.v4.runtime.tree.RuleNode
import org.opensearch.flint.spark.FlintSpark
import org.opensearch.flint.spark.covering.FlintSparkCoveringIndex
import org.opensearch.flint.spark.refresh.FlintSparkIndexRefresh.SchedulerMode
import org.opensearch.flint.spark.sql.{FlintSparkSqlCommand, FlintSparkSqlExtensionsVisitor, SparkSqlAstBuilder}
import org.opensearch.flint.spark.sql.FlintSparkSqlAstBuilder.{getFullTableName, getSqlText}
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
      val tableName = getFullTableName(flint, ctx.tableName)
      val indexBuilder =
        flint
          .coveringIndex()
          .name(indexName)
          .onTable(tableName)

      ctx.indexColumns.multipartIdentifierProperty().forEach { indexColCtx =>
        val colName = indexColCtx.multipartIdentifier().getText
        indexBuilder.addIndexColumns(colName)
      }

      if (ctx.whereClause() != null) {
        indexBuilder.filterBy(getSqlText(ctx.whereClause().filterCondition()))
      }

      val ignoreIfExists = ctx.EXISTS() != null
      val indexOptions = visitPropertyList(ctx.propertyList())
      indexBuilder
        .options(indexOptions)
        .create(ignoreIfExists)

      // Trigger auto refresh if enabled and not using external scheduler
      if (indexOptions
          .autoRefresh() && SchedulerMode.INTERNAL == indexOptions.schedulerMode()) {
        val flintIndexName = getFlintIndexName(flint, ctx.indexName, ctx.tableName)
        flint.refreshIndex(flintIndexName)
      }
      Seq.empty
    }
  }

  override def visitRefreshCoveringIndexStatement(
      ctx: RefreshCoveringIndexStatementContext): Command = {
    FlintSparkSqlCommand() { flint =>
      val flintIndexName = getFlintIndexName(flint, ctx.indexName, ctx.tableName)
      flint.refreshIndex(flintIndexName)
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
        .collect {
          case index: FlintSparkCoveringIndex if index.tableName == fullTableName =>
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

  override def visitAlterCoveringIndexStatement(
      ctx: AlterCoveringIndexStatementContext): Command = {
    FlintSparkSqlCommand() { flint =>
      val indexName = getFlintIndexName(flint, ctx.indexName, ctx.tableName)
      val indexOptions = visitPropertyList(ctx.propertyList())
      val index = flint
        .describeIndex(indexName)
        .getOrElse(throw new IllegalStateException(s"Index $indexName doesn't exist"))
      val updatedIndex = flint.coveringIndex().copyWithUpdate(index, indexOptions)
      flint.updateIndex(updatedIndex)
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

  override def visitVacuumCoveringIndexStatement(
      ctx: VacuumCoveringIndexStatementContext): Command = {
    FlintSparkSqlCommand() { flint =>
      val flintIndexName = getFlintIndexName(flint, ctx.indexName, ctx.tableName)
      flint.vacuumIndex(flintIndexName)
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
