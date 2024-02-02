/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.skipping.bloomfilter

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import org.opensearch.flint.spark.skipping.bloomfilter.BloomFilter.{BLOOM_FILTER_ALGORITHM_KEY, BloomFilterFactory}
import org.opensearch.flint.spark.skipping.bloomfilter.BloomFilter.Algorithm.CLASSIC
import org.opensearch.flint.spark.skipping.bloomfilter.ClassicBloomFilter.{CLASSIC_BLOOM_FILTER_FPP_KEY, CLASSIC_BLOOM_FILTER_NUM_ITEMS_KEY, DEFAULT_CLASSIC_BLOOM_FILTER_FPP, DEFAULT_CLASSIC_BLOOM_FILTER_NUM_ITEMS}
import org.scalatest.matchers.should.Matchers

import org.apache.spark.FlintSuite

class ClassicBloomFilterSuite extends FlintSuite with Matchers {

  test("parameters should return all parameters including defaults") {
    val factory = new BloomFilterFactory(Map(BLOOM_FILTER_ALGORITHM_KEY -> CLASSIC.toString))

    factory.parameters should contain allOf (BLOOM_FILTER_ALGORITHM_KEY -> CLASSIC.toString,
    CLASSIC_BLOOM_FILTER_NUM_ITEMS_KEY -> DEFAULT_CLASSIC_BLOOM_FILTER_NUM_ITEMS.toString,
    CLASSIC_BLOOM_FILTER_FPP_KEY -> DEFAULT_CLASSIC_BLOOM_FILTER_FPP.toString)
  }

  test("parameters should return all specified parameters") {
    val expectedNumItems = 50000
    val fpp = 0.001
    val factory = new BloomFilterFactory(
      Map(
        BLOOM_FILTER_ALGORITHM_KEY -> CLASSIC.toString,
        CLASSIC_BLOOM_FILTER_NUM_ITEMS_KEY -> expectedNumItems.toString,
        CLASSIC_BLOOM_FILTER_FPP_KEY -> fpp.toString))

    factory.parameters should contain allOf (BLOOM_FILTER_ALGORITHM_KEY -> CLASSIC.toString,
    CLASSIC_BLOOM_FILTER_NUM_ITEMS_KEY -> expectedNumItems.toString,
    CLASSIC_BLOOM_FILTER_FPP_KEY -> fpp.toString)
  }

  test("serialize and deserialize") {
    val factory = new BloomFilterFactory(Map(BLOOM_FILTER_ALGORITHM_KEY -> CLASSIC.toString))
    val bloomFilter = factory.create()
    bloomFilter.put(1L)
    bloomFilter.put(2L)
    bloomFilter.put(3L)

    // Serialize and then deserialize should remain the same
    val out = new ByteArrayOutputStream()
    bloomFilter.writeTo(out)
    val in = new ByteArrayInputStream(out.toByteArray)
    val newBloomFilter = factory.deserialize(in)
    bloomFilter shouldBe newBloomFilter
  }
}
