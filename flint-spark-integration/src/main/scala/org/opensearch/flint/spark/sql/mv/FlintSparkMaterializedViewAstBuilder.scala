/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.sql.mv

import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

import org.antlr.v4.runtime.tree.RuleNode
import org.opensearch.flint.spark.FlintSpark
import org.opensearch.flint.spark.mv.FlintSparkMaterializedView
import org.opensearch.flint.spark.sql.{FlintSparkSqlCommand, FlintSparkSqlExtensionsVisitor, IndexMetricHelper, SparkSqlAstBuilder}
import org.opensearch.flint.spark.sql.FlintSparkSqlAstBuilder.{getFullTableName, getSqlText, IndexBelongsTo}
import org.opensearch.flint.spark.sql.FlintSparkSqlExtensionsParser._

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.Command
import org.apache.spark.sql.types.StringType

/**
 * Flint Spark AST builder that builds Spark command for Flint materialized view statement.
 */
trait FlintSparkMaterializedViewAstBuilder
    extends FlintSparkSqlExtensionsVisitor[AnyRef]
    with IndexMetricHelper {
  self: SparkSqlAstBuilder =>

  override def visitCreateMaterializedViewStatement(
      ctx: CreateMaterializedViewStatementContext): Command = {
    FlintSparkSqlCommand() { flint =>
      val mvName = getFullTableName(flint, ctx.mvName)
      val query = getSqlText(ctx.query)

      val mvBuilder = flint
        .materializedView()
        .name(mvName)
        .query(query)

      val ignoreIfExists = ctx.EXISTS() != null
      val indexOptions = visitPropertyList(ctx.propertyList())
      val flintIndexName = getFlintIndexName(flint, ctx.mvName)

      emitCreateIndexMetric(indexOptions.autoRefresh())

      mvBuilder
        .options(indexOptions, flintIndexName)
        .create(ignoreIfExists)

      // Trigger auto refresh if enabled and not using external scheduler
      if (indexOptions
          .autoRefresh() && !mvBuilder.isExternalSchedulerEnabled()) {
        flint.refreshIndex(flintIndexName)
      }
      Seq.empty
    }
  }

  override def visitRefreshMaterializedViewStatement(
      ctx: RefreshMaterializedViewStatementContext): Command = {
    FlintSparkSqlCommand() { flint =>
      emitRefreshIndexMetric()
      val flintIndexName = getFlintIndexName(flint, ctx.mvName)
      flint.refreshIndex(flintIndexName)
      Seq.empty
    }
  }

  override def visitShowMaterializedViewStatement(
      ctx: ShowMaterializedViewStatementContext): Command = {
    val outputSchema = Seq(
      AttributeReference("materialized_view_name", StringType, nullable = false)())

    FlintSparkSqlCommand(outputSchema) { flint =>
      val catalogDbName =
        ctx.catalogDb.parts
          .map(part => part.getText)
          .mkString("_")
      val indexNamePattern = s"flint_${catalogDbName}_*"
      flint
        .describeIndexes(indexNamePattern)
        .collect {
          // Ensure index is a MV within the given catalog and database
          case mv: FlintSparkMaterializedView if mv belongsTo ctx.catalogDb =>
            // MV name must be qualified when metadata created
            Row(mv.mvName.split('.').drop(2).mkString("."))
        }
    }
  }

  override def visitDescribeMaterializedViewStatement(
      ctx: DescribeMaterializedViewStatementContext): Command = {
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

  override def visitAlterMaterializedViewStatement(
      ctx: AlterMaterializedViewStatementContext): Command = {
    FlintSparkSqlCommand() { flint =>
      emitAlterIndexMetric()
      val indexName = getFlintIndexName(flint, ctx.mvName)
      val indexOptions = visitPropertyList(ctx.propertyList())
      val index = flint
        .describeIndex(indexName)
        .getOrElse(throw new IllegalStateException(s"Index $indexName doesn't exist"))
      val updatedIndex = flint.materializedView().copyWithUpdate(index, indexOptions)
      flint.updateIndex(updatedIndex)
      Seq.empty
    }
  }

  override def visitDropMaterializedViewStatement(
      ctx: DropMaterializedViewStatementContext): Command = {
    FlintSparkSqlCommand() { flint =>
      emitDropIndexMetric()
      flint.deleteIndex(getFlintIndexName(flint, ctx.mvName))
      Seq.empty
    }
  }

  override def visitVacuumMaterializedViewStatement(
      ctx: VacuumMaterializedViewStatementContext): Command = {
    FlintSparkSqlCommand() { flint =>
      emitVacuumIndexMetric()
      flint.vacuumIndex(getFlintIndexName(flint, ctx.mvName))
      Seq.empty
    }
  }

  private def getFlintIndexName(flint: FlintSpark, mvNameCtx: RuleNode): String = {
    val fullMvName = getFullTableName(flint, mvNameCtx)
    FlintSparkMaterializedView.getFlintIndexName(fullMvName)
  }
}
