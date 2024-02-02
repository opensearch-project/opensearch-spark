/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.skipping.bloomfilter

import java.io.{InputStream, OutputStream}
import java.util.Locale

import org.opensearch.flint.spark.skipping.bloomfilter.BloomFilter.Algorithm.{Algorithm, CLASSIC}

/**
 * Bloom filter interface inspired by [[org.apache.spark.util.sketch.BloomFilter]] but adapts to
 * Flint skipping index use and remove unnecessary API for now.
 */
trait BloomFilter {

  /**
   * @return
   *   algorithm kind
   */
  def algorithm: Algorithm

  /**
   * @return
   *   the number of bits in the underlying bit array.
   */
  def bitSize(): Long

  /**
   * Put an item into this bloom filter.
   *
   * @param item
   *   Long value item to insert
   * @return
   *   true if bits changed which means the item must be first time added to the bloom filter.
   *   Otherwise, it maybe the first time or not.
   */
  def put(item: Long): Boolean

  /**
   * Merge this bloom filter with another bloom filter.
   *
   * @param bloomFilter
   *   bloom filter to merge
   * @return
   *   bloom filter after merged
   */
  def merge(bloomFilter: BloomFilter): BloomFilter

  /**
   * @param item
   *   Long value item to check
   * @return
   *   true if the item may exist in this bloom filter. Otherwise, it is definitely not exist.
   */
  def mightContain(item: Long): Boolean

  /**
   * Serialize this bloom filter and write it to an output stream.
   *
   * @param out
   *   output stream to write
   */
  def writeTo(out: OutputStream): Unit
}

object BloomFilter {

  /**
   * Bloom filter algorithm.
   */
  object Algorithm extends Enumeration {
    type Algorithm = Value
    val CLASSIC = Value
  }

  /**
   * Bloom filter algorithm parameter name and default value if not present.
   */
  val BLOOM_FILTER_ALGORITHM_KEY = "algorithm"
  val DEFAULT_BLOOM_FILTER_ALGORITHM = CLASSIC.toString

  /**
   * Bloom filter factory that instantiate concrete bloom filter implementation.
   *
   * @param params
   *   bloom filter algorithm parameters
   */
  class BloomFilterFactory(params: Map[String, String]) extends Serializable {

    /**
     * Bloom filter algorithm specified in parameters.
     */
    private val algorithm: Algorithm = {
      val param = params.getOrElse(BLOOM_FILTER_ALGORITHM_KEY, DEFAULT_BLOOM_FILTER_ALGORITHM)
      Algorithm.withName(param.toUpperCase(Locale.ROOT))
    }

    /**
     * Get all bloom filter parameters used to store in index metadata.
     *
     * @return
     *   all bloom filter algorithm parameters including those not present but has default values.
     */
    def parameters: Map[String, String] = {
      algorithm match {
        case CLASSIC => ClassicBloomFilter.getParameters(params) // TODO: add algorithm param
      }
    }

    /**
     * Create a concrete bloom filter according to the parameters.
     *
     * @return
     *   bloom filter instance
     */
    def create(): BloomFilter = {
      algorithm match {
        case CLASSIC => new ClassicBloomFilter(parameters)
      }
    }

    /**
     * Deserialize to create the bloom filter.
     *
     * @param in
     *   input stream to read from
     * @return
     *   bloom filter instance
     */
    def deserialize(in: InputStream): BloomFilter = {
      algorithm match {
        case CLASSIC => ClassicBloomFilter.deserialize(in)
      }
    }
  }
}
