/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.skipping.valueset

import org.opensearch.flint.spark.skipping.FlintSparkSkippingStrategy
import org.opensearch.flint.spark.skipping.FlintSparkSkippingStrategy.SkippingKind.{SkippingKind, VALUE_SET}
import org.opensearch.flint.spark.skipping.valueset.ValueSetSkippingStrategy.DEFAULT_VALUE_SET_SIZE_LIMIT

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, Expression, Literal}
import org.apache.spark.sql.functions._

/**
 * Skipping strategy based on unique column value set.
 */
case class ValueSetSkippingStrategy(
    override val kind: SkippingKind = VALUE_SET,
    override val columnName: String,
    override val columnType: String)
    extends FlintSparkSkippingStrategy {

  override def outputSchema(): Map[String, String] =
    Map(columnName -> columnType)

  override def getAggregators: Seq[Expression] = {
    val limit = DEFAULT_VALUE_SET_SIZE_LIMIT
    val collectSet = collect_set(columnName)
    val aggregator =
      when(size(collectSet) > limit, lit(null))
        .otherwise(collectSet)
    Seq(aggregator.expr)
  }

  override def rewritePredicate(predicate: Expression): Option[Expression] =
    /*
     * This is supposed to be rewritten to ARRAY_CONTAINS(columName, value).
     * However, due to push down limitation in Spark, we keep the equation.
     */
    predicate match {
      case EqualTo(AttributeReference(`columnName`, _, _, _), value: Literal) =>
        // Value set maybe null due to maximum size limit restriction
        Some((isnull(col(columnName)) || col(columnName) === value).expr)
      case _ => None
    }
}

object ValueSetSkippingStrategy {

  /**
   * Default limit for value set size collected. TODO: make this val once it's configurable
   */
  var DEFAULT_VALUE_SET_SIZE_LIMIT = 100
}
