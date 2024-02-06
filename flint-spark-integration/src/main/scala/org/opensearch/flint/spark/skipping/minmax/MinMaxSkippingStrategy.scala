/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.skipping.minmax

import org.opensearch.flint.spark.skipping.FlintSparkSkippingStrategy
import org.opensearch.flint.spark.skipping.FlintSparkSkippingStrategy.IndexExpressionMatcher
import org.opensearch.flint.spark.skipping.FlintSparkSkippingStrategy.SkippingKind.{MIN_MAX, SkippingKind}

import org.apache.spark.sql.catalyst.expressions.{And, EqualTo, Expression, GreaterThan, GreaterThanOrEqual, In, LessThan, LessThanOrEqual, Literal}
import org.apache.spark.sql.catalyst.expressions.aggregate.{Max, Min}
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.functions.col

/**
 * Skipping strategy based on min-max boundary of column values.
 */
case class MinMaxSkippingStrategy(
    override val kind: SkippingKind = MIN_MAX,
    override val columnName: String,
    override val columnType: String)
    extends FlintSparkSkippingStrategy {

  /** Column name in Flint index data. */
  private def minColName = s"MinMax_${columnName}_0"
  private def maxColName = s"MinMax_${columnName}_1"

  override def outputSchema(): Map[String, String] =
    Map(minColName -> columnType, maxColName -> columnType)

  override def getAggregators: Seq[Expression] = {
    Seq(
      Min(col(columnName).expr).toAggregateExpression(),
      Max(col(columnName).expr).toAggregateExpression())
  }

  override def rewritePredicate(predicate: Expression): Option[Expression] = {
    val IndexExpression = IndexExpressionMatcher(columnName)
    predicate match {
      case EqualTo(IndexExpression(_), value: Literal) =>
        Some((col(minColName) <= value && col(maxColName) >= value).expr)
      case LessThan(IndexExpression(_), value: Literal) =>
        Some((col(minColName) < value).expr)
      case LessThanOrEqual(IndexExpression(_), value: Literal) =>
        Some((col(minColName) <= value).expr)
      case GreaterThan(IndexExpression(_), value: Literal) =>
        Some((col(maxColName) > value).expr)
      case GreaterThanOrEqual(IndexExpression(_), value: Literal) =>
        Some((col(maxColName) >= value).expr)
      case In(column @ IndexExpression(_), AllLiterals(literals)) =>
        /*
         * First, convert IN to approximate range check: min(in_list) <= col <= max(in_list)
         * to avoid long and maybe unnecessary comparison expressions.
         */
        val values = literals.map(_.value)
        val ordering = TypeUtils.getInterpretedOrdering(column.dataType)
        val minVal = values.min(ordering)
        val maxVal = values.max(ordering)
        Some(
          And(
            rewritePredicate(GreaterThanOrEqual(column, Literal(minVal))).get,
            rewritePredicate(LessThanOrEqual(column, Literal(maxVal))).get))
      case _ => None
    }
  }

  /** Need this because Scala pattern match doesn't work for generic type like Seq[Literal] */
  object AllLiterals {
    def unapply(values: Seq[Expression]): Option[Seq[Literal]] = {
      if (values.forall(_.isInstanceOf[Literal])) {
        Some(values.asInstanceOf[Seq[Literal]])
      } else {
        None
      }
    }
  }
}
