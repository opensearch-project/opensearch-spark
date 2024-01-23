/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.skipping.partition

import org.opensearch.flint.spark.skipping.FlintSparkSkippingStrategy
import org.opensearch.flint.spark.skipping.FlintSparkSkippingStrategy.IndexExpressionMatcher
import org.opensearch.flint.spark.skipping.FlintSparkSkippingStrategy.SkippingKind.{PARTITION, SkippingKind}
import org.opensearch.flint.spark.utils.ExpressionUtils.parseExprString

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.{EqualTo, Expression, Literal}
import org.apache.spark.sql.catalyst.expressions.aggregate.First
import org.apache.spark.sql.functions.col

/**
 * Skipping strategy for partitioned columns of source table.
 */
case class PartitionSkippingStrategy(
    override val kind: SkippingKind = PARTITION,
    override val columnName: String,
    override val columnType: String)
    extends FlintSparkSkippingStrategy {

  override def outputSchema(): Map[String, String] = {
    Map(columnName -> columnType)
  }

  override def getAggregators: Seq[Expression] = {
    Seq(First(col(columnName).expr, ignoreNulls = true).toAggregateExpression())
  }

  override def doRewritePredicate(
      predicate: Expression,
      indexExpr: Expression): Option[Expression] = {
    // Use unresolved expr, otherwise Spark treats rewritten plan as corrupted
    val indexCol = new Column(parseExprString(columnName))
    val IndexExpression = IndexExpressionMatcher(indexExpr)

    predicate match {
      // Column has same name in index data, so just rewrite to the same equation
      case EqualTo(IndexExpression(_), value: Literal) =>
        Some((indexCol === value).expr)
      case _ => None
    }
  }
}
