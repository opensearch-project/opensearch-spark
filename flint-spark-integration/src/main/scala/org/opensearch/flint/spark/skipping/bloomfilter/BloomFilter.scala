/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.skipping.bloomfilter

import java.io.{InputStream, OutputStream}
import java.util.Locale

import org.opensearch.flint.spark.skipping.bloomfilter.BloomFilter.Algorithm.{Algorithm, CLASSIC}

trait BloomFilter {

  def algorithm: Algorithm

  def bitSize(): Long

  def put(item: Long): Boolean

  def merge(bloomFilter: BloomFilter): BloomFilter

  def mightContain(item: Long): Boolean

  def writeTo(out: OutputStream): Unit
}

object BloomFilter {

  object Algorithm extends Enumeration {
    type Algorithm = Value
    val CLASSIC = Value
  }

  val BLOOM_FILTER_ALGORITHM_KEY = "algorithm"
  val DEFAULT_BLOOM_FILTER_ALGORITHM = CLASSIC.toString

  class BloomFilterFactory(params: Map[String, String]) extends Serializable {

    private val algorithm: Algorithm = {
      val param = params.getOrElse(BLOOM_FILTER_ALGORITHM_KEY, DEFAULT_BLOOM_FILTER_ALGORITHM)
      Algorithm.withName(param.toUpperCase(Locale.ROOT))
    }

    def parameters: Map[String, String] = {
      algorithm match {
        case CLASSIC => ClassicBloomFilter.getParameters(params) // TODO: add algorithm param
      }
    }

    def create(): BloomFilter = {
      algorithm match {
        case CLASSIC => new ClassicBloomFilter(parameters)
      }
    }

    def deserialize(in: InputStream): BloomFilter = {
      algorithm match {
        case CLASSIC => ClassicBloomFilter.deserialize(in)
      }
    }
  }
}
