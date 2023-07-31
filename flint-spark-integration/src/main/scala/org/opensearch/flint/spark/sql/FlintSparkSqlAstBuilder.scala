/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.sql

import org.antlr.v4.runtime.tree.RuleNode
import org.opensearch.flint.spark.FlintSpark
import org.opensearch.flint.spark.FlintSpark.RefreshMode
import org.opensearch.flint.spark.skipping.FlintSparkSkippingIndex
import org.opensearch.flint.spark.skipping.FlintSparkSkippingStrategy.SkippingKind
import org.opensearch.flint.spark.skipping.FlintSparkSkippingStrategy.SkippingKind.{MIN_MAX, PARTITION, VALUE_SET}
import org.opensearch.flint.spark.sql.FlintSparkSqlExtensionsParser._

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.Command
import org.apache.spark.sql.types.StringType

/**
 * Flint Spark AST builder that builds Spark command for Flint index statement.
 */
class FlintSparkSqlAstBuilder extends FlintSparkSqlExtensionsBaseVisitor[Command] {

  override def visitCreateSkippingIndexStatement(
      ctx: CreateSkippingIndexStatementContext): Command =
    FlintSparkSqlCommand() { flint =>
      // Create skipping index
      val indexBuilder = flint
        .skippingIndex()
        .onTable(getFullTableName(flint, ctx.tableName))

      ctx.indexColTypeList().indexColType().forEach { colTypeCtx =>
        val colName = colTypeCtx.identifier().getText
        val skipType = SkippingKind.withName(colTypeCtx.skipType.getText)
        skipType match {
          case PARTITION => indexBuilder.addPartitions(colName)
          case VALUE_SET => indexBuilder.addValueSet(colName)
          case MIN_MAX => indexBuilder.addMinMax(colName)
        }
      }
      indexBuilder.create()

      // Trigger auto refresh if enabled
      if (isAutoRefreshEnabled(ctx.propertyList())) {
        val indexName = getSkippingIndexName(flint, ctx.tableName)
        flint.refreshIndex(indexName, RefreshMode.INCREMENTAL)
      }
      Seq.empty
    }

  override def visitRefreshSkippingIndexStatement(
      ctx: RefreshSkippingIndexStatementContext): Command =
    FlintSparkSqlCommand() { flint =>
      val indexName = getSkippingIndexName(flint, ctx.tableName)
      flint.refreshIndex(indexName, RefreshMode.FULL)
      Seq.empty
    }

  override def visitDescribeSkippingIndexStatement(
      ctx: DescribeSkippingIndexStatementContext): Command = {
    val outputSchema = Seq(
      AttributeReference("indexed_col_name", StringType, nullable = false)(),
      AttributeReference("data_type", StringType, nullable = false)(),
      AttributeReference("skip_type", StringType, nullable = false)())

    FlintSparkSqlCommand(outputSchema) { flint =>
      val indexName = getSkippingIndexName(flint, ctx.tableName)
      flint
        .describeIndex(indexName)
        .map { case index: FlintSparkSkippingIndex =>
          index.indexedColumns.map(strategy =>
            Row(strategy.columnName, strategy.columnType, strategy.kind.toString))
        }
        .getOrElse(Seq.empty)
    }
  }

  override def visitDropSkippingIndexStatement(ctx: DropSkippingIndexStatementContext): Command =
    FlintSparkSqlCommand() { flint =>
      val indexName = getSkippingIndexName(flint, ctx.tableName)
      flint.deleteIndex(indexName)
      Seq.empty
    }

  private def isAutoRefreshEnabled(ctx: PropertyListContext): Boolean = {
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

  private def getSkippingIndexName(flint: FlintSpark, tableNameCtx: RuleNode): String =
    FlintSparkSkippingIndex.getSkippingIndexName(getFullTableName(flint, tableNameCtx))

  private def getFullTableName(flint: FlintSpark, tableNameCtx: RuleNode): String = {
    val tableName = tableNameCtx.getText
    if (tableName.contains(".")) {
      tableName
    } else {
      val db = flint.spark.catalog.currentDatabase
      s"$db.$tableName"
    }
  }

  override def aggregateResult(aggregate: Command, nextResult: Command): Command =
    if (nextResult != null) nextResult else aggregate
}
