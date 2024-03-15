/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.sql.index

import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

import org.opensearch.flint.spark.covering.FlintSparkCoveringIndex
import org.opensearch.flint.spark.mv.FlintSparkMaterializedView
import org.opensearch.flint.spark.skipping.FlintSparkSkippingIndex
import org.opensearch.flint.spark.sql.{FlintSparkSqlCommand, FlintSparkSqlExtensionsVisitor, SparkSqlAstBuilder}
import org.opensearch.flint.spark.sql.FlintSparkSqlExtensionsParser.ShowFlintIndexStatementContext

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.Command
import org.apache.spark.sql.types.{BooleanType, StringType}

/**
 * Flint Spark AST builder that builds Spark command for Flint index management statement.
 */
trait FlintSparkIndexAstBuilder extends FlintSparkSqlExtensionsVisitor[AnyRef] {
  self: SparkSqlAstBuilder =>

  override def visitShowFlintIndexStatement(ctx: ShowFlintIndexStatementContext): Command = {
    val outputSchema = Seq(
      AttributeReference("flint_index_name", StringType, nullable = false)(),
      AttributeReference("kind", StringType, nullable = false)(),
      AttributeReference("database", StringType, nullable = false)(),
      AttributeReference("table", StringType, nullable = true)(),
      AttributeReference("index_name", StringType, nullable = true)(),
      AttributeReference("auto_refresh", BooleanType, nullable = false)(),
      AttributeReference("status", StringType, nullable = false)())

    FlintSparkSqlCommand(outputSchema) { flint =>
      val catalogDbName =
        ctx.catalogDb.parts
          .map(part => part.getText)
          .mkString("_")
      val indexNamePattern = s"flint_${catalogDbName}_*"
      flint
        .describeIndexes(indexNamePattern)
        .map { index =>
          val (databaseName, tableName, indexName) = index match {
            case skipping: FlintSparkSkippingIndex =>
              val parts = skipping.tableName.split('.')
              (parts(1), parts.drop(2).mkString("."), null)
            case covering: FlintSparkCoveringIndex =>
              val parts = covering.tableName.split('.')
              (parts(1), parts.drop(2).mkString("."), covering.indexName)
            case mv: FlintSparkMaterializedView =>
              val parts = mv.mvName.split('.')
              (parts(1), null, parts.drop(2).mkString("."))
          }

          val status = index.latestLogEntry match {
            case Some(entry) => entry.state.toString
            case None => "unavailable"
          }

          Row(
            index.name,
            index.kind,
            databaseName,
            tableName,
            indexName,
            index.options.autoRefresh(),
            status)
        }
    }
  }
}
