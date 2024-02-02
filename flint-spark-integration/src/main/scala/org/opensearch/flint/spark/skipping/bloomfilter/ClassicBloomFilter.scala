/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.skipping.bloomfilter
import java.io.{InputStream, OutputStream}

import org.opensearch.flint.spark.skipping.bloomfilter.BloomFilter.Algorithm.{Algorithm, CLASSIC}
import org.opensearch.flint.spark.skipping.bloomfilter.ClassicBloomFilter.{CLASSIC_BLOOM_FILTER_FPP_KEY, CLASSIC_BLOOM_FILTER_NUM_ITEMS_KEY}

class ClassicBloomFilter(val delegate: org.apache.spark.util.sketch.BloomFilter)
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

  val CLASSIC_BLOOM_FILTER_NUM_ITEMS_KEY = "num_items"
  val DEFAULT_CLASSIC_BLOOM_FILTER_NUM_ITEMS = 10000

  val CLASSIC_BLOOM_FILTER_FPP_KEY = "fpp"
  val DEFAULT_CLASSIC_BLOOM_FILTER_FPP = 0.01

  def getParameters(params: Map[String, String]): Map[String, String] = {
    val map = Map.newBuilder[String, String]
    map ++= params

    if (!params.contains(CLASSIC_BLOOM_FILTER_NUM_ITEMS_KEY)) {
      map += (CLASSIC_BLOOM_FILTER_NUM_ITEMS_KEY -> DEFAULT_CLASSIC_BLOOM_FILTER_NUM_ITEMS.toString)
    }
    if (!params.contains(CLASSIC_BLOOM_FILTER_FPP_KEY)) {
      map += (CLASSIC_BLOOM_FILTER_FPP_KEY -> DEFAULT_CLASSIC_BLOOM_FILTER_FPP.toString)
    }
    map.result()
  }

  def deserialize(in: InputStream): BloomFilter = {
    val delegate = org.apache.spark.util.sketch.BloomFilter.readFrom(in)
    new ClassicBloomFilter(delegate)
  }
}
