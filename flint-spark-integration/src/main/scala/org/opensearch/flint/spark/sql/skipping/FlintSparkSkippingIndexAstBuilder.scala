/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.sql.skipping

import scala.collection.JavaConverters.collectionAsScalaIterableConverter

import org.antlr.v4.runtime.tree.RuleNode
import org.opensearch.flint.spark.FlintSpark
import org.opensearch.flint.spark.skipping.FlintSparkSkippingIndex
import org.opensearch.flint.spark.skipping.FlintSparkSkippingStrategy.SkippingKind
import org.opensearch.flint.spark.skipping.FlintSparkSkippingStrategy.SkippingKind.{MIN_MAX, PARTITION, VALUE_SET}
import org.opensearch.flint.spark.skipping.valueset.ValueSetSkippingStrategy.VALUE_SET_MAX_SIZE_KEY
import org.opensearch.flint.spark.sql.{FlintSparkSqlCommand, FlintSparkSqlExtensionsVisitor, SparkSqlAstBuilder}
import org.opensearch.flint.spark.sql.FlintSparkSqlAstBuilder.{getFullTableName, getSqlText}
import org.opensearch.flint.spark.sql.FlintSparkSqlExtensionsParser._

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.Command
import org.apache.spark.sql.types.{ArrayType, MapType, StringType}

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
      AttributeReference("rule", StringType, nullable = false)(),
      AttributeReference("recommendation", ArrayType(MapType(StringType, StringType)), nullable = false)(),
      AttributeReference("unsupported columns", ArrayType(StringType), nullable = false)())

    FlintSparkSqlCommand(outputSchema) { flint =>
      Seq(
          Row("all top-level columns",
            List(
              Map("column_name"->"year", "column_type"->"DateType", "skipping_type"->"PARTITION", "reason"->"top level partition column"),
              Map("column_name"->"month", "column_type"->"StringType", "skipping_type"->"BLOOMFILTER", "reason"->"top level string column"),
              Map("column_name"->"day", "column_type"->"IntegerType", "skipping_type"->"MIN_MAX", "reason"->"top level integer column"),
              Map("column_name"->"hour", "column_type"->"TimestampType", "skipping_type"->"PARTITION", "reason"->"top level partition column")
            ),
            List("binary_code", "map_column")),
          Row("all top-level columns and nested columns",
            List(
              Map("column_name"->"year", "column_type"->"DateType", "skipping_type"->"PARTITION", "reason"->"top level partition column"),
              Map("column_name"->"month", "column_type"->"StringType", "skipping_type"->"BLOOMFILTER", "reason"->"top level string column"),
              Map("column_name"->"day", "column_type"->"IntegerType", "skipping_type"->"MIN_MAX", "reason"->"top level integer column"),
              Map("column_name"->"hour", "column_type"->"TimestampType", "skipping_type"->"PARTITION", "reason"->"top level partition column")
            ),
            List("array column", "decimal value")),
          Row("first 32 columns",
            List(
              Map("column_name"->"year", "column_type"->"DateType", "skipping_type"->"PARTITION", "reason"->"top level partition column"),
              Map("column_name"->"month", "column_type"->"StringType", "skipping_type"->"BLOOMFILTER", "reason"->"top level string column"),
              Map("column_name"->"day", "column_type"->"IntegerType", "skipping_type"->"MIN_MAX", "reason"->"top level integer column"),
              Map("column_name"->"hour", "column_type"->"TimestampType", "skipping_type"->"PARTITION", "reason"->"top level partition column")
            ),
            List())
        )
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
