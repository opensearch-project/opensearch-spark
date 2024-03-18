/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.sql.skipping

import scala.collection.JavaConverters.collectionAsScalaIterableConverter

import org.antlr.v4.runtime.tree.RuleNode
import org.opensearch.flint.core.field.bloomfilter.BloomFilterFactory._
import org.opensearch.flint.spark.FlintSpark
import org.opensearch.flint.spark.skipping.FlintSparkSkippingIndex
import org.opensearch.flint.spark.skipping.FlintSparkSkippingStrategy.SkippingKind
import org.opensearch.flint.spark.skipping.FlintSparkSkippingStrategy.SkippingKind.{BLOOM_FILTER, MIN_MAX, PARTITION, VALUE_SET}
import org.opensearch.flint.spark.skipping.valueset.ValueSetSkippingStrategy.VALUE_SET_MAX_SIZE_KEY
import org.opensearch.flint.spark.sql.{FlintSparkSqlCommand, FlintSparkSqlExtensionsVisitor, SparkSqlAstBuilder}
import org.opensearch.flint.spark.sql.FlintSparkSqlAstBuilder.{getFullTableName, getSqlText}
import org.opensearch.flint.spark.sql.FlintSparkSqlExtensionsParser._

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.Command
import org.apache.spark.sql.types.StringType

/**
 * Flint Spark AST builder that builds Spark command for Flint skipping index statement.
 */
trait FlintSparkSkippingIndexAstBuilder extends FlintSparkSqlExtensionsVisitor[AnyRef] {
  self: SparkSqlAstBuilder =>

  override def visitCreateSkippingIndexStatement(
      ctx: CreateSkippingIndexStatementContext): Command =
    FlintSparkSqlCommand() { flint =>
      // TODO: support filtering condition
      if (ctx.whereClause() != null) {
        throw new UnsupportedOperationException(
          s"Filtering condition is not supported: ${getSqlText(ctx.whereClause())}")
      }

      // Create skipping index
      val indexBuilder = flint
        .skippingIndex()
        .onTable(getFullTableName(flint, ctx.tableName))

      ctx.indexColTypeList().indexColType().forEach { colTypeCtx =>
        val colName = colTypeCtx.identifier().getText
        val skipType = SkippingKind.withName(colTypeCtx.skipType.getText)
        val skipParams = visitSkipParams(colTypeCtx.skipParams())
        skipType match {
          case PARTITION => indexBuilder.addPartitions(colName)
          case VALUE_SET =>
            val valueSetParams = (Seq(VALUE_SET_MAX_SIZE_KEY) zip skipParams).toMap
            indexBuilder.addValueSet(colName, valueSetParams)
          case MIN_MAX => indexBuilder.addMinMax(colName)
          case BLOOM_FILTER =>
            // Determine if the given parameters are for adaptive algorithm by the first parameter
            val bloomFilterParamKeys =
              if (skipParams.headOption.exists(_.equalsIgnoreCase("false"))) {
                Seq(
                  BLOOM_FILTER_ADAPTIVE_KEY,
                  CLASSIC_BLOOM_FILTER_NUM_ITEMS_KEY,
                  CLASSIC_BLOOM_FILTER_FPP_KEY)
              } else {
                Seq(ADAPTIVE_NUMBER_CANDIDATE_KEY, CLASSIC_BLOOM_FILTER_FPP_KEY)
              }
            val bloomFilterParams = (bloomFilterParamKeys zip skipParams).toMap
            indexBuilder.addBloomFilter(colName, bloomFilterParams)
        }
      }

      val ignoreIfExists = ctx.EXISTS() != null
      val indexOptions = visitPropertyList(ctx.propertyList())
      indexBuilder
        .options(indexOptions)
        .create(ignoreIfExists)

      // Trigger auto refresh if enabled
      if (indexOptions.autoRefresh()) {
        val indexName = getSkippingIndexName(flint, ctx.tableName)
        flint.refreshIndex(indexName)
      }
      Seq.empty
    }

  override def visitRefreshSkippingIndexStatement(
      ctx: RefreshSkippingIndexStatementContext): Command =
    FlintSparkSqlCommand() { flint =>
      val indexName = getSkippingIndexName(flint, ctx.tableName)
      flint.refreshIndex(indexName)
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

  override def visitAnalyzeSkippingIndexStatement(
      ctx: AnalyzeSkippingIndexStatementContext): Command = {

    val outputSchema = Seq(
      AttributeReference("column_name", StringType, nullable = false)(),
      AttributeReference("column_type", StringType, nullable = false)(),
      AttributeReference("reason", StringType, nullable = false)(),
      AttributeReference("skipping_type", StringType, nullable = false)())

    FlintSparkSqlCommand(outputSchema) { flint =>
      flint.analyzeSkippingIndex(ctx.tableName().getText)
    }
  }

  override def visitDropSkippingIndexStatement(ctx: DropSkippingIndexStatementContext): Command =
    FlintSparkSqlCommand() { flint =>
      val indexName = getSkippingIndexName(flint, ctx.tableName)
      flint.deleteIndex(indexName)
      Seq.empty
    }

  override def visitVacuumSkippingIndexStatement(
      ctx: VacuumSkippingIndexStatementContext): Command = {
    FlintSparkSqlCommand() { flint =>
      val indexName = getSkippingIndexName(flint, ctx.tableName)
      flint.vacuumIndex(indexName)
      Seq.empty
    }
  }

  override def visitSkipParams(ctx: SkipParamsContext): Seq[String] = {
    if (ctx == null) {
      Seq.empty
    } else {
      ctx.propertyValue.asScala
        .map(p => visitPropertyValue(p))
        .toSeq
    }
  }

  private def getSkippingIndexName(flint: FlintSpark, tableNameCtx: RuleNode): String =
    FlintSparkSkippingIndex.getSkippingIndexName(getFullTableName(flint, tableNameCtx))
}
