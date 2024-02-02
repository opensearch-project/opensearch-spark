/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.skipping.bloomfilter

import org.opensearch.flint.spark.skipping.FlintSparkSkippingStrategy
import org.opensearch.flint.spark.skipping.FlintSparkSkippingStrategy.SkippingKind.{BLOOM_FILTER, SkippingKind}
import org.opensearch.flint.spark.skipping.bloomfilter.BloomFilter.BloomFilterFactory

import org.apache.spark.sql.catalyst.expressions.Expression
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

  private val bloomFilterFactory = new BloomFilterFactory(params)

  override val parameters: Map[String, String] = bloomFilterFactory.parameters

  override def outputSchema(): Map[String, String] = Map(columnName -> "binary") // TODO: binary?

  override def getAggregators: Seq[Expression] = {
    Seq(
      new BloomFilterAgg(xxhash64(col(columnName)).expr, bloomFilterFactory)
        .toAggregateExpression())
  }

  override def rewritePredicate(predicate: Expression): Option[Expression] = None
}
