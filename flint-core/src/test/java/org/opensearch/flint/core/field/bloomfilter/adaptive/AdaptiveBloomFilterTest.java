/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.field.bloomfilter.adaptive;

import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.opensearch.flint.core.field.bloomfilter.BloomFilter;

public class AdaptiveBloomFilterTest {

  private final AdaptiveBloomFilter bloomFilter = new AdaptiveBloomFilter(0.01);

  @Test
  public void test() {
    for (int i = 0; i < 10; i++) {
      bloomFilter.put(i);
    }
    BloomFilter candidate1 = bloomFilter.bestCandidate();

    for (int i = 50; i < 200; i++) {
      bloomFilter.put(i);
    }
    BloomFilter candidate2 = bloomFilter.bestCandidate();
    assertTrue(candidate1.bitSize() < candidate2.bitSize());

    for (int i = 300; i < 1000; i++) {
      bloomFilter.put(i);
    }
    BloomFilter candidate3 = bloomFilter.bestCandidate();
    assertTrue(candidate2.bitSize() < candidate3.bitSize());
  }
}