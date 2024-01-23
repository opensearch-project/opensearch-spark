/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.skipping.valueset

import org.opensearch.flint.spark.skipping.FlintSparkSkippingStrategy
import org.opensearch.flint.spark.skipping.FlintSparkSkippingStrategy.IndexColumnExtractor
import org.opensearch.flint.spark.skipping.FlintSparkSkippingStrategy.SkippingKind.{SkippingKind, VALUE_SET}
import org.opensearch.flint.spark.skipping.valueset.ValueSetSkippingStrategy.{DEFAULT_VALUE_SET_MAX_SIZE, VALUE_SET_MAX_SIZE_KEY}

import org.apache.spark.sql.catalyst.expressions.{EqualTo, Expression, Literal}
import org.apache.spark.sql.functions._

/**
 * Skipping strategy based on unique column value set.
 */
case class ValueSetSkippingStrategy(
    override val kind: SkippingKind = VALUE_SET,
    override val columnName: String,
    override val columnType: String,
    params: Map[String, String] = Map.empty)
    extends FlintSparkSkippingStrategy {

  override val parameters: Map[String, String] = {
    val map = Map.newBuilder[String, String]
    map ++= params

    if (!params.contains(VALUE_SET_MAX_SIZE_KEY)) {
      map += (VALUE_SET_MAX_SIZE_KEY -> DEFAULT_VALUE_SET_MAX_SIZE.toString)
    }
    map.result()
  }

  override def outputSchema(): Map[String, String] =
    Map(columnName -> columnType)

  override def getAggregators: Seq[Expression] = {
    val limit = parameters(VALUE_SET_MAX_SIZE_KEY).toInt
    val collectSet = collect_set(columnName)
    val aggregator =
      when(size(collectSet) > limit, lit(null))
        .otherwise(collectSet)
    Seq(aggregator.expr)
  }

  override def doRewritePredicate(
      predicate: Expression,
      indexExpr: Expression): Option[Expression] = {
    val extractor = IndexColumnExtractor(columnName, indexExpr)

    /*
     * This is supposed to be rewritten to ARRAY_CONTAINS(columName, value).
     * However, due to push down limitation in Spark, we keep the equation.
     */
    predicate match {
      case EqualTo(extractor(col), value: Literal) =>
        // Value set maybe null due to maximum size limit restriction
        Some((isnull(col) || col === value).expr)
      case _ => None
    }
  }
}

object ValueSetSkippingStrategy {

  /** Value set max size param key and default value */
  var VALUE_SET_MAX_SIZE_KEY = "max_size"
  var DEFAULT_VALUE_SET_MAX_SIZE = 100
}
