/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.field.bloomfilter.adaptive;

import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.opensearch.flint.core.field.bloomfilter.adaptive.AdaptiveBloomFilter.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import org.junit.Test;
import org.opensearch.flint.core.field.bloomfilter.BloomFilter;

public class AdaptiveBloomFilterTest {

  private final int numCandidates = 10;

  private final AdaptiveBloomFilter bloomFilter = new AdaptiveBloomFilter(numCandidates, 0.01);

  @Test
  public void shouldChooseBestCandidateAdaptively() {
    // Insert 500 items
    for (int i = 0; i < 500; i++) {
      bloomFilter.put(i);
    }
    BloomFilterCandidate candidate1 = bloomFilter.bestCandidate();
    assertEquals(1024, candidate1.expectedNumItems);

    // Insert 1000 (total 1500)
    for (int i = 500; i < 1500; i++) {
      bloomFilter.put(i);
    }
    BloomFilterCandidate candidate2 = bloomFilter.bestCandidate();
    assertEquals(2048, candidate2.expectedNumItems);

    // Insert 4000 (total 5500)
    for (int i = 1500; i < 5500; i++) {
      bloomFilter.put(i);
    }
    BloomFilterCandidate candidate3 = bloomFilter.bestCandidate();
    assertEquals(8192, candidate3.expectedNumItems);
  }

  @Test
  public void shouldBeTheSameAfterWriteToAndReadFrom() throws IOException {
    bloomFilter.put(123L);
    bloomFilter.put(456L);
    bloomFilter.put(789L);

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    bloomFilter.writeTo(out);
    InputStream in = new ByteArrayInputStream(out.toByteArray());
    BloomFilter newBloomFilter = readFrom(numCandidates, in);
    assertEquals(bloomFilter, newBloomFilter);
  }
}