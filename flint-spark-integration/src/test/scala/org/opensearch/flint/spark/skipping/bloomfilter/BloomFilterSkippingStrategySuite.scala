/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.skipping.bloomfilter

import org.opensearch.flint.spark.skipping.{FlintSparkSkippingStrategy, FlintSparkSkippingStrategySuite}
import org.opensearch.flint.spark.skipping.bloomfilter.BloomFilter.Algorithm.CLASSIC
import org.opensearch.flint.spark.skipping.bloomfilter.BloomFilter.BLOOM_FILTER_ALGORITHM_KEY
import org.opensearch.flint.spark.skipping.bloomfilter.ClassicBloomFilter.{CLASSIC_BLOOM_FILTER_FPP_KEY, CLASSIC_BLOOM_FILTER_NUM_ITEMS_KEY, DEFAULT_CLASSIC_BLOOM_FILTER_FPP, DEFAULT_CLASSIC_BLOOM_FILTER_NUM_ITEMS}
import org.scalatest.matchers.should.Matchers

import org.apache.spark.FlintSuite

class BloomFilterSkippingStrategySuite
    extends FlintSuite
    with FlintSparkSkippingStrategySuite
    with Matchers {

  /** Subclass initializes strategy class to test */
  override val strategy: FlintSparkSkippingStrategy =
    BloomFilterSkippingStrategy(columnName = "name", columnType = "string")

  test("parameters") {
    strategy.parameters should contain allOf (BLOOM_FILTER_ALGORITHM_KEY -> CLASSIC.toString,
    CLASSIC_BLOOM_FILTER_NUM_ITEMS_KEY -> DEFAULT_CLASSIC_BLOOM_FILTER_NUM_ITEMS.toString,
    CLASSIC_BLOOM_FILTER_FPP_KEY -> DEFAULT_CLASSIC_BLOOM_FILTER_FPP.toString)
  }
}
