/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.skipping.bloomfilter

import org.opensearch.flint.core.field.bloomfilter.BloomFilterFactory._
import org.opensearch.flint.spark.skipping.{FlintSparkSkippingStrategy, FlintSparkSkippingStrategySuite}
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
    strategy.parameters shouldBe Map(
      BLOOM_FILTER_ADAPTIVE_KEY -> DEFAULT_BLOOM_FILTER_ADAPTIVE.toString,
      ADAPTIVE_NUMBER_CANDIDATE_KEY -> DEFAULT_ADAPTIVE_NUMBER_CANDIDATE.toString,
      CLASSIC_BLOOM_FILTER_FPP_KEY -> DEFAULT_CLASSIC_BLOOM_FILTER_FPP.toString)
  }
}
