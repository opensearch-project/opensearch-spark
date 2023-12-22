/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.skipping.valueset

import org.opensearch.flint.spark.function.CollectSetLimit
import org.opensearch.flint.spark.skipping.FlintSparkSkippingStrategy
import org.opensearch.flint.spark.skipping.FlintSparkSkippingStrategy.SkippingKind.{SkippingKind, VALUE_SET}

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, Expression, Literal}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateFunction, CollectSet}
import org.apache.spark.sql.functions.col

/**
 * Skipping strategy based on unique column value set.
 */
case class ValueSetSkippingStrategy(
    override val kind: SkippingKind = VALUE_SET,
    override val columnName: String,
    override val columnType: String,
    limit: Int = 100)
    extends FlintSparkSkippingStrategy {

  override def outputSchema(): Map[String, String] =
    Map(columnName -> columnType)

  override def getAggregators: Seq[AggregateFunction] = {
    val expr = col(columnName).expr
    if (limit == 0) {
      Seq(CollectSet(expr))
    } else {
      Seq(CollectSetLimit(expr, limit))
    }
  }

  override def rewritePredicate(predicate: Expression): Option[Expression] =
    /*
     * This is supposed to be rewritten to ARRAY_CONTAINS(columName, value).
     * However, due to push down limitation in Spark, we keep the equation.
     */
    predicate match {
      case EqualTo(AttributeReference(`columnName`, _, _, _), value: Literal) =>
        Some((col(columnName) === value).expr)
      case _ => None
    }
}
