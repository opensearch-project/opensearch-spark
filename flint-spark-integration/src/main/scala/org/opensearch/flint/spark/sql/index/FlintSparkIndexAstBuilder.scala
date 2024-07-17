/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.sql.index

import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

import org.opensearch.flint.spark.FlintSparkIndex
import org.opensearch.flint.spark.covering.FlintSparkCoveringIndex
import org.opensearch.flint.spark.mv.FlintSparkMaterializedView
import org.opensearch.flint.spark.skipping.FlintSparkSkippingIndex
import org.opensearch.flint.spark.sql.{FlintSparkSqlCommand, FlintSparkSqlExtensionsVisitor, SparkSqlAstBuilder}
import org.opensearch.flint.spark.sql.FlintSparkSqlAstBuilder.IndexBelongsTo
import org.opensearch.flint.spark.sql.FlintSparkSqlExtensionsParser.{MultipartIdentifierContext, ShowFlintIndexStatementContext}

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.Command
import org.apache.spark.sql.types.{BooleanType, StringType}

/**
 * Flint Spark AST builder that builds Spark command for Flint index management statement.
 */
trait FlintSparkIndexAstBuilder extends FlintSparkSqlExtensionsVisitor[AnyRef] {
  self: SparkSqlAstBuilder =>

  /**
   * Represents the basic output schema for the FlintSparkSqlCommand. This schema includes
   * essential information about each index, such as:
   *   - Index Name (`flint_index_name`)
   *   - Index Kind (`kind`)
   *   - Associated Database (`database`)
   *   - Table the index is associated with (`table`), which can be nullable
   *   - Specific Index Name (`index_name`), which can be nullable
   *   - Auto-refresh status (`auto_refresh`)
   *   - Current status of the index (`status`)
   */
  private val baseOutputSchema = Seq(
    AttributeReference("flint_index_name", StringType, nullable = false)(),
    AttributeReference("kind", StringType, nullable = false)(),
    AttributeReference("database", StringType, nullable = false)(),
    AttributeReference("table", StringType, nullable = true)(),
    AttributeReference("index_name", StringType, nullable = true)(),
    AttributeReference("auto_refresh", BooleanType, nullable = false)(),
    AttributeReference("status", StringType, nullable = false)())

  /**
   * Extends the base output schema with additional information. This schema is used when the
   * EXTENDED keyword is present which includes:
   *   - Error information (`error`), detailing any errors associated with the index, which can be
   *     nullable.
   */
  private val extendedOutputSchema = Seq(
    AttributeReference("error", StringType, nullable = true)())

  override def visitShowFlintIndexStatement(ctx: ShowFlintIndexStatementContext): Command = {
    if (ctx.EXTENDED() == null) {
      new ShowFlintIndexCommandBuilder()
        .withSchema(baseOutputSchema)
        .forCatalog(ctx.catalogDb)
        .constructRows(baseRowData)
        .build()
    } else {
      new ShowFlintIndexCommandBuilder()
        .withSchema(baseOutputSchema ++ extendedOutputSchema)
        .forCatalog(ctx.catalogDb)
        .constructRows(index => baseRowData(index) ++ extendedRowData(index))
        .build()
    }
  }

  /**
   * Builder class for constructing FlintSparkSqlCommand objects.
   */
  private class ShowFlintIndexCommandBuilder {
    private var schema: Seq[AttributeReference] = _
    private var catalogDb: MultipartIdentifierContext = _
    private var rowDataBuilder: FlintSparkIndex => Seq[Any] = _

    /** Specify the output schema for the command. */
    def withSchema(schema: Seq[AttributeReference]): ShowFlintIndexCommandBuilder = {
      this.schema = schema
      this
    }

    /** Specify the catalog database context for the command. */
    def forCatalog(catalogDb: MultipartIdentifierContext): ShowFlintIndexCommandBuilder = {
      this.catalogDb = catalogDb
      this
    }

    /** Configures a function to construct row data for each index. */
    def constructRows(
        rowDataBuilder: FlintSparkIndex => Seq[Any]): ShowFlintIndexCommandBuilder = {
      this.rowDataBuilder = rowDataBuilder
      this
    }

    /** Builds the command using the configured parameters. */
    def build(): FlintSparkSqlCommand = {
      require(schema != null, "Schema must be set before building the command")
      require(catalogDb != null, "Catalog database must be set before building the command")
      require(rowDataBuilder != null, "Row data builder must be set before building the command")

      FlintSparkSqlCommand(schema) { flint =>
        val catalogDbName =
          catalogDb.parts
            .map(part => part.getText)
            .mkString("_")
        val indexNamePattern = s"flint_${catalogDbName}_*"

        flint
          .describeIndexes(indexNamePattern)
          .filter(index => index belongsTo catalogDb)
          .map { index => Row.fromSeq(rowDataBuilder(index)) }
      }
    }
  }

  private def baseRowData(index: FlintSparkIndex): Seq[Any] = {
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

    Seq(
      index.name,
      index.kind,
      databaseName,
      tableName,
      indexName,
      index.options.autoRefresh(),
      status)
  }

  private def extendedRowData(index: FlintSparkIndex): Seq[Any] = {
    val error = index.latestLogEntry match {
      case Some(entry) => entry.error
      case None => null
    }
    Seq(error)
  }
}
