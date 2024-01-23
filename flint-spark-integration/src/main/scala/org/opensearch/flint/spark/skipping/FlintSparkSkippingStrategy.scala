/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.skipping

import org.json4s.CustomSerializer
import org.json4s.JsonAST.JString
import org.opensearch.flint.spark.skipping.FlintSparkSkippingStrategy.SkippingKind.SkippingKind
import org.opensearch.flint.spark.utils.ExpressionUtils.resolveExprString

import org.apache.spark.sql.catalyst.expressions.{And, Expression}
import org.apache.spark.sql.catalyst.plans.logical.Filter

/**
 * Skipping index strategy that defines skipping data structure building and reading logic.
 */
trait FlintSparkSkippingStrategy {

  /**
   * Skipping strategy kind.
   */
  val kind: SkippingKind

  /**
   * Indexed column name (can be a simple name or expression).
   */
  val columnName: String

  /**
   * Indexed column Spark SQL type.
   */
  val columnType: String

  /**
   * Skipping algorithm named parameters.
   */
  val parameters: Map[String, String] = Map.empty

  /**
   * @return
   *   output schema mapping from Flint field name to Flint field type
   */
  def outputSchema(): Map[String, String]

  /**
   * @return
   *   aggregators that generate skipping data structure
   */
  def getAggregators: Seq[Expression]

  /**
   * Rewrite a filtering condition on source table into a new predicate on index data based on
   * current skipping strategy.
   *
   * @param filter
   *   filter operator on source table
   * @return
   *   new filtering condition on index data or empty if index not applicable
   */
  def rewritePredicate(filter: Filter): Option[Expression] = {
    // Traverse all expressions in the predicate and try to rewrite it
    def rewriteExpression(
        condition: Expression,
        indexExpr: Expression): Option[Expression] = condition match {
      case and: And =>
        and.children
          .flatMap(child => rewriteExpression(child, indexExpr))
          .reduceOption(And)
      case expr => doRewritePredicate(expr, indexExpr)
    }

    val indexExpr = resolveExprString(columnName, filter.child)
    rewriteExpression(filter.condition, indexExpr)
  }

  // Visible for UT
  def doRewritePredicate(predicate: Expression, indexExpr: Expression): Option[Expression]
}

object FlintSparkSkippingStrategy {

  /**
   * Skipping kind enum class.
   */
  object SkippingKind extends Enumeration {
    type SkippingKind = Value

    // Use Value[s]Set because ValueSet already exists in Enumeration
    val PARTITION, VALUE_SET, MIN_MAX = Value
  }

  /** json4s doesn't serialize Enum by default */
  object SkippingKindSerializer
      extends CustomSerializer[SkippingKind](_ =>
        (
          { case JString(value) =>
            SkippingKind.withName(value)
          },
          { case kind: SkippingKind =>
            JString(kind.toString)
          }))

  /**
   * Extractor that extracts the given expression if it matches the index expression in skipping
   * index.
   *
   * @param indexExpr
   *   index expression in a skipping indexed column
   */
  case class IndexExpressionMatcher(indexExpr: Expression) {

    def unapply(expr: Expression): Option[Expression] = {
      if (expr.semanticEquals(indexExpr)) {
        Some(expr)
      } else {
        None
      }
    }
  }
}
