/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.query

import org.opensearch.sql.api.{UnifiedQueryContext, UnifiedQueryPlanner}
import org.opensearch.sql.api.transpiler.UnifiedQueryTranspiler
import org.opensearch.sql.common.antlr.SyntaxCheckException
import org.opensearch.sql.executor.QueryType
import org.opensearch.sql.ppl.calcite.OpenSearchSparkSqlDialect

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types.{DataType, StructType}

/**
 * A custom Spark SQL parser that delegates query parsing and planning to the Unified Query
 * Planner. It converts unified queries into Spark SQL queries for execution, and falls back to
 * the default Spark parser when the input query is not supported.
 *
 * @param context
 *   The unified query context (by-name parameter for lazy evaluation)
 * @param sparkParser
 *   The underlying Spark SQL parser to delegate non-PPL queries
 */
class UnifiedQuerySparkParser(context: => UnifiedQueryContext, sparkParser: ParserInterface)
    extends ParserInterface
    with Logging {

  /** Lazily initialized planner that converts PPL queries to unified logical plans. */
  private lazy val unifiedQueryPlanner = new UnifiedQueryPlanner(context)

  /** Transpiler that converts unified plan to Spark SQL using Spark SQL dialect. */
  private val sparkSqlTranspiler = UnifiedQueryTranspiler
    .builder()
    .dialect(OpenSearchSparkSqlDialect.DEFAULT)
    .build()

  /**
   * Parses a query string into a Spark LogicalPlan. Attempts to parse as PPL and transpile to
   * Spark SQL. If parsing fails with a SyntaxCheckException, delegates to the underlying Spark
   * parser.
   *
   * @param query
   *   The query string (PPL or SQL)
   * @return
   *   Spark LogicalPlan
   */
  override def parsePlan(query: String): LogicalPlan = {
    try {
      val unifiedPlan = unifiedQueryPlanner.plan(query)
      val sqlText = sparkSqlTranspiler.toSql(unifiedPlan)
      sparkParser.parsePlan(sqlText)
    } catch {
      // Fall back to Spark parser if unified query planner cannot handle
      case _: SyntaxCheckException => sparkParser.parsePlan(query)
    }
  }

  // Delegate all other ParserInterface methods to the underlying Spark parser

  override def parseExpression(sqlText: String): Expression =
    sparkParser.parseExpression(sqlText)

  override def parseTableIdentifier(sqlText: String): TableIdentifier =
    sparkParser.parseTableIdentifier(sqlText)

  override def parseFunctionIdentifier(sqlText: String): FunctionIdentifier =
    sparkParser.parseFunctionIdentifier(sqlText)

  override def parseMultipartIdentifier(sqlText: String): Seq[String] =
    sparkParser.parseMultipartIdentifier(sqlText)

  override def parseTableSchema(sqlText: String): StructType =
    sparkParser.parseTableSchema(sqlText)

  override def parseDataType(sqlText: String): DataType =
    sparkParser.parseDataType(sqlText)

  override def parseQuery(sqlText: String): LogicalPlan =
    sparkParser.parseQuery(sqlText)
}

/** Companion object for UnifiedQuerySparkParser providing factory and utility method. */
object UnifiedQuerySparkParser {

  /**
   * Creates a UnifiedQuerySparkParser with the given Spark session and parser.
   *
   * @param spark
   *   The SparkSession
   * @param sparkParser
   *   The underlying Spark SQL parser
   * @return
   *   UnifiedQuerySparkParser instance
   */
  def apply(spark: SparkSession, sparkParser: ParserInterface): UnifiedQuerySparkParser = {
    new UnifiedQuerySparkParser(buildContext(spark), sparkParser)
  }

  private def buildContext(spark: SparkSession): UnifiedQueryContext = {
    val currentCatalog = spark.catalog.currentCatalog
    val currentDatabase = spark.catalog.currentDatabase
    val builder =
      UnifiedQueryContext
        .builder()
        .language(QueryType.PPL)
        .defaultNamespace(s"$currentCatalog.$currentDatabase")

    // Register all available catalogs
    spark.sessionState.catalogManager
      .listCatalogs(None)
      .foreach(catalogName => {
        builder.catalog(catalogName, new SparkSchema(spark, catalogName))
      })

    // Enable unrestricted query capabilities for now
    builder
      .setting("plugins.calcite.all_join_types.allowed", true)
      .setting("plugins.ppl.subsearch.maxout", 0)
      .setting("plugins.ppl.join.subsearch_maxout", 0)
      .build()
  }
}
