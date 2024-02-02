/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.skipping.bloomfilter
import java.io.{InputStream, OutputStream}

import org.opensearch.flint.spark.skipping.bloomfilter.BloomFilter.Algorithm.{Algorithm, CLASSIC}
import org.opensearch.flint.spark.skipping.bloomfilter.BloomFilter.BLOOM_FILTER_ALGORITHM_KEY
import org.opensearch.flint.spark.skipping.bloomfilter.ClassicBloomFilter.{CLASSIC_BLOOM_FILTER_FPP_KEY, CLASSIC_BLOOM_FILTER_NUM_ITEMS_KEY}

/**
 * Classic bloom filter implementation by reusing Spark built-in bloom filter.
 *
 * @param delegate
 *   Spark bloom filter instance
 */
case class ClassicBloomFilter(delegate: org.apache.spark.util.sketch.BloomFilter)
    extends BloomFilter
    with Serializable {

  def this(params: Map[String, String]) = {
    this(
      org.apache.spark.util.sketch.BloomFilter
        .create(
          params(CLASSIC_BLOOM_FILTER_NUM_ITEMS_KEY).toLong,
          params(CLASSIC_BLOOM_FILTER_FPP_KEY).toDouble))
  }

  override def algorithm: Algorithm = CLASSIC

  override def bitSize(): Long = delegate.bitSize()

  override def put(item: Long): Boolean = delegate.putLong(item)

  override def merge(bloomFilter: BloomFilter): BloomFilter = {
    delegate.mergeInPlace(bloomFilter.asInstanceOf[ClassicBloomFilter].delegate)
    this
  }

  override def mightContain(item: Long): Boolean = delegate.mightContainLong(item)

  override def writeTo(out: OutputStream): Unit = delegate.writeTo(out)
}

object ClassicBloomFilter {

  /**
   * Expected number of unique items key and default value.
   */
  val CLASSIC_BLOOM_FILTER_NUM_ITEMS_KEY = "num_items"
  val DEFAULT_CLASSIC_BLOOM_FILTER_NUM_ITEMS = 10000

  /**
   * False positive probability (FPP) key and default value.
   */
  val CLASSIC_BLOOM_FILTER_FPP_KEY = "fpp"
  val DEFAULT_CLASSIC_BLOOM_FILTER_FPP = 0.01

  /**
   * @param params
   *   given parameters
   * @return
   *   all parameters including those not present but has default value
   */
  def getParameters(params: Map[String, String]): Map[String, String] = {
    val map = Map.newBuilder[String, String]
    map ++= params

    if (!params.contains(BLOOM_FILTER_ALGORITHM_KEY)) {
      map += (BLOOM_FILTER_ALGORITHM_KEY -> CLASSIC.toString)
    }
    if (!params.contains(CLASSIC_BLOOM_FILTER_NUM_ITEMS_KEY)) {
      map += (CLASSIC_BLOOM_FILTER_NUM_ITEMS_KEY -> DEFAULT_CLASSIC_BLOOM_FILTER_NUM_ITEMS.toString)
    }
    if (!params.contains(CLASSIC_BLOOM_FILTER_FPP_KEY)) {
      map += (CLASSIC_BLOOM_FILTER_FPP_KEY -> DEFAULT_CLASSIC_BLOOM_FILTER_FPP.toString)
    }
    map.result()
  }

  /**
   * Deserialize and instantiate a classic bloom filter instance.
   *
   * @param in
   *   input stream to read from
   * @return
   *   classic bloom filter instance
   */
  def deserialize(in: InputStream): BloomFilter = {
    val delegate = org.apache.spark.util.sketch.BloomFilter.readFrom(in)
    new ClassicBloomFilter(delegate)
  }
}
