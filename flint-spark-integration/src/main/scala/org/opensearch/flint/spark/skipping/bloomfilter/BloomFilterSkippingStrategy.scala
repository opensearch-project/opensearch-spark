/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.skipping.bloomfilter

import org.opensearch.flint.spark.skipping.FlintSparkSkippingStrategy
import org.opensearch.flint.spark.skipping.FlintSparkSkippingStrategy.IndexColumnExtractor
import org.opensearch.flint.spark.skipping.FlintSparkSkippingStrategy.SkippingKind.{BLOOM_FILTER, SkippingKind}
import org.opensearch.flint.spark.skipping.bloomfilter.BloomFilterSkippingStrategy.{CLASSIC_BLOOM_FILTER_FPP_KEY, CLASSIC_BLOOM_FILTER_NUM_ITEMS_KEY, DEFAULT_CLASSIC_BLOOM_FILTER_FPP, DEFAULT_CLASSIC_BLOOM_FILTER_NUM_ITEMS}

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.{EqualTo, Expression, Literal}
import org.apache.spark.sql.functions.{col, xxhash64}

/**
 * Skipping strategy based on approximate data structure bloom filter.
 */
case class BloomFilterSkippingStrategy(
    override val kind: SkippingKind = BLOOM_FILTER,
    override val columnName: String,
    override val columnType: String,
    params: Map[String, String] = Map.empty)
    extends FlintSparkSkippingStrategy {

  override val parameters: Map[String, String] = {
    Map(
      CLASSIC_BLOOM_FILTER_NUM_ITEMS_KEY -> expectedNumItems.toString,
      CLASSIC_BLOOM_FILTER_FPP_KEY -> fpp.toString)
  }

  override def outputSchema(): Map[String, String] = Map(columnName -> "binary")

  override def getAggregators: Seq[Expression] = {
    Seq(
      new BloomFilterAgg(xxhash64(col(columnName)).expr, expectedNumItems, fpp)
        .toAggregateExpression()
    ) // TODO: use xxhash64() for now
  }

  override def rewritePredicate(predicate: Expression): Option[Expression] = {
    val IndexColumn = IndexColumnExtractor(columnName)
    predicate match {
      case EqualTo(IndexColumn(indexCol), value: Literal) =>
        Some(BloomFilterMightContain(indexCol.expr, xxhash64(new Column(value)).expr))
      case _ => None
    }
  }

  private def expectedNumItems: Int = {
    params
      .get(CLASSIC_BLOOM_FILTER_NUM_ITEMS_KEY)
      .map(_.toInt)
      .getOrElse(DEFAULT_CLASSIC_BLOOM_FILTER_NUM_ITEMS)
  }

  private def fpp: Double = {
    params
      .get(CLASSIC_BLOOM_FILTER_FPP_KEY)
      .map(_.toDouble)
      .getOrElse(DEFAULT_CLASSIC_BLOOM_FILTER_FPP)
  }
}

object BloomFilterSkippingStrategy {

  /**
   * Expected number of unique items key and default value.
   */
  val CLASSIC_BLOOM_FILTER_NUM_ITEMS_KEY = "num_items"
  val DEFAULT_CLASSIC_BLOOM_FILTER_NUM_ITEMS = 10000

  /**
   * False positive probability (FPP) key and default value.
   */
  val CLASSIC_BLOOM_FILTER_FPP_KEY = "fpp"
  val DEFAULT_CLASSIC_BLOOM_FILTER_FPP = 0.03
}
