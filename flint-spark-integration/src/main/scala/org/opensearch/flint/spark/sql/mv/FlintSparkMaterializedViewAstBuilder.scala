/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.sql.mv

import org.antlr.v4.runtime.tree.RuleNode
import org.opensearch.flint.spark.{FlintSpark, FlintSparkIndex}
import org.opensearch.flint.spark.FlintSpark.RefreshMode
import org.opensearch.flint.spark.mv.FlintSparkMaterializedView
import org.opensearch.flint.spark.sql.{FlintSparkSqlCommand, FlintSparkSqlExtensionsVisitor, SparkSqlAstBuilder}
import org.opensearch.flint.spark.sql.FlintSparkSqlAstBuilder.getFullTableName
import org.opensearch.flint.spark.sql.FlintSparkSqlExtensionsParser._

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.trees.CurrentOrigin
import org.apache.spark.sql.types.StringType

/**
 * Flint Spark AST builder that builds Spark command for Flint materialized view statement.
 */
trait FlintSparkMaterializedViewAstBuilder extends FlintSparkSqlExtensionsVisitor[AnyRef] {
  self: SparkSqlAstBuilder =>

  override def visitCreateMaterializedViewStatement(
      ctx: CreateMaterializedViewStatementContext): AnyRef = {
    FlintSparkSqlCommand() { flint =>
      val mvName = getFullTableName(flint, ctx.mvName)
      val query = getMvQuery(ctx.query)

      val mvBuilder = flint
        .materializedView()
        .name(mvName)
        .query(query)

      val ignoreIfExists = ctx.EXISTS() != null
      val indexOptions = visitPropertyList(ctx.propertyList())
      mvBuilder
        .options(indexOptions)
        .create(ignoreIfExists)

      // Trigger auto refresh if enabled
      if (indexOptions.autoRefresh()) {
        val flintIndexName = getFlintIndexName(flint, ctx.mvName)
        flint.refreshIndex(flintIndexName, RefreshMode.INCREMENTAL)
      }
      Seq.empty
    }
  }

  override def visitShowMaterializedViewStatement(
      ctx: ShowMaterializedViewStatementContext): AnyRef = {
    val outputSchema = Seq(
      AttributeReference("materialized_view_name", StringType, nullable = false)())

    FlintSparkSqlCommand(outputSchema) { flint =>
      val catalogDbName = ctx.catalogDb.getText
      val indexNamePattern = FlintSparkIndex.flintIndexNamePrefix(catalogDbName) + "*"
      flint
        .describeIndexes(indexNamePattern)
        .collect { case mv: FlintSparkMaterializedView =>
          Row(mv.mvName)
        }
    }
  }

  override def visitDescribeMaterializedViewStatement(
      ctx: DescribeMaterializedViewStatementContext): AnyRef = {
    val outputSchema = Seq(
      AttributeReference("output_col_name", StringType, nullable = false)(),
      AttributeReference("data_type", StringType, nullable = false)())

    FlintSparkSqlCommand(outputSchema) { flint =>
      val flintIndexName = getFlintIndexName(flint, ctx.mvName)
      flint
        .describeIndex(flintIndexName)
        .map { case mv: FlintSparkMaterializedView =>
          mv.outputSchema.map { case (colName, colType) =>
            Row(colName, colType)
          }.toSeq
        }
        .getOrElse(Seq.empty)
    }
  }

  override def visitDropMaterializedViewStatement(
      ctx: DropMaterializedViewStatementContext): AnyRef = {
    FlintSparkSqlCommand() { flint =>
      flint.deleteIndex(getFlintIndexName(flint, ctx.mvName))
      Seq.empty
    }
  }

  private def getMvQuery(ctx: MaterializedViewQueryContext): String = {
    // Assume origin must be preserved at the beginning of parsing
    val sqlText = CurrentOrigin.get.sqlText.get
    val startIndex = ctx.getStart.getStartIndex
    val stopIndex = ctx.getStop.getStopIndex
    sqlText.substring(startIndex, stopIndex + 1)
  }

  private def getFlintIndexName(flint: FlintSpark, mvNameCtx: RuleNode): String = {
    val fullMvName = getFullTableName(flint, mvNameCtx)
    FlintSparkMaterializedView.getFlintIndexName(fullMvName)
  }
}
