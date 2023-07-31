/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.skipping.partition

import org.opensearch.flint.spark.skipping.FlintSparkSkippingStrategy
import org.opensearch.flint.spark.skipping.FlintSparkSkippingStrategy.SkippingKind.{PARTITION, SkippingKind}

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, Expression, Literal}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateFunction, First}
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

  override def getAggregators: Seq[AggregateFunction] = {
    Seq(First(col(columnName).expr, ignoreNulls = true))
  }

  override def rewritePredicate(predicate: Expression): Option[Expression] =
    predicate match {
      // Column has same name in index data, so just rewrite to the same equation
      case EqualTo(AttributeReference(`columnName`, _, _, _), value: Literal) =>
        Some((col(columnName) === value).expr)
      case _ => None
    }
}
