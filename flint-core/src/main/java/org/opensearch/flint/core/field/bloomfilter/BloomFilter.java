/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.field.bloomfilter;

import java.io.OutputStream;

/**
 * Bloom filter interface inspired by [[org.apache.spark.util.sketch.BloomFilter]] but adapts to
 * Flint skipping index use and remove unnecessary API for now.
 */
public interface BloomFilter {

  /**
   * @return the number of bits in the underlying bit array.
   */
  long bitSize();

  /**
   * Put an item into this bloom filter.
   *
   * @param item Long value item to insert
   * @return true if bits changed which means the item must be first time added to the bloom filter.
   * Otherwise, it maybe the first time or not.
   */
  boolean put(long item);

  /**
   * Merge this bloom filter with another bloom filter.
   *
   * @param bloomFilter bloom filter to merge
   * @return bloom filter after merged
   */
  BloomFilter merge(BloomFilter bloomFilter);

  /**
   * @param item Long value item to check
   * @return true if the item may exist in this bloom filter. Otherwise, it is definitely not exist.
   */
  boolean mightContain(long item);

  /**
   * Serialize this bloom filter and write it to an output stream.
   *
   * @param out output stream to write
   */
  void writeTo(OutputStream out);
}
