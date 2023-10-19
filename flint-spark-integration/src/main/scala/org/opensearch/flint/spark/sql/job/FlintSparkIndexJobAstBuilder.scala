/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.sql.job

import org.json4s.{Formats, NoTypeHints}
import org.json4s.native.Serialization
import org.opensearch.flint.spark.FlintSparkIndex
import org.opensearch.flint.spark.sql.{FlintSparkSqlCommand, FlintSparkSqlExtensionsVisitor, SparkSqlAstBuilder}
import org.opensearch.flint.spark.sql.FlintSparkSqlExtensionsParser.ShowIndexJobStatementContext

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.types.StringType

/**
 * Flint Spark AST builder that builds Spark command for Flint index job management statement.
 */
trait FlintSparkIndexJobAstBuilder extends FlintSparkSqlExtensionsVisitor[AnyRef] {
  self: SparkSqlAstBuilder =>

  override def visitShowIndexJobStatement(ctx: ShowIndexJobStatementContext): AnyRef = {
    val outputSchema = Seq(
      AttributeReference("job_name", StringType, nullable = false)(),
      AttributeReference("index_type", StringType, nullable = false)(),
      AttributeReference("index_name", StringType, nullable = false)(),
      AttributeReference("source", StringType, nullable = false)(),
      AttributeReference("properties", StringType, nullable = false)())

    FlintSparkSqlCommand(outputSchema) { flint =>
      val indexNamePattern = "flint_*"
      flint
        .describeIndexes(indexNamePattern)
        .collect {
          case index: FlintSparkIndex if index.options.autoRefresh() =>
            val metadata = index.metadata()
            Row(
              index.name(),
              metadata.kind,
              metadata.name,
              metadata.source,
              jsonify(metadata.properties))
        }
    }
  }

  private def jsonify(map: java.util.Map[String, AnyRef]): String = {
    implicit val formats: Formats = Serialization.formats(NoTypeHints)
    Serialization.write(map)
  }
}
