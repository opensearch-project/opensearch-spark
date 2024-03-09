/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.field.bloomfilter.adaptive;

import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.opensearch.flint.core.field.bloomfilter.adaptive.AdaptiveBloomFilter.readFrom;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import org.junit.Test;
import org.opensearch.flint.core.field.bloomfilter.BloomFilter;

public class AdaptiveBloomFilterTest {

  private final int numCandidates = 5;

  private final AdaptiveBloomFilter bloomFilter = new AdaptiveBloomFilter(numCandidates, 0.03);

  @Test
  public void shouldChooseBestCandidateAdaptively() {
    // Insert 500 items should choose 1st candidate
    for (int i = 0; i < 500; i++) {
      bloomFilter.put(i);
    }
    assertEquals(1024, bloomFilter.bestCandidate().expectedNumItems);

    // Insert 1000 (total 1500) should choose 2nd candidate
    for (int i = 500; i < 1500; i++) {
      bloomFilter.put(i);
    }
    assertEquals(2048, bloomFilter.bestCandidate().expectedNumItems);

    // Insert 4000 (total 5500) should choose 4th candidate
    for (int i = 1500; i < 5500; i++) {
      bloomFilter.put(i);
    }
    assertEquals(8192, bloomFilter.bestCandidate().expectedNumItems);
  }

  @Test
  public void shouldBeTheSameAfterWriteToAndReadFrom() throws IOException {
    bloomFilter.put(123L);
    bloomFilter.put(456L);
    bloomFilter.put(789L);

    // Serialize and deserialize and assert the equality
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    bloomFilter.writeTo(out);
    InputStream in = new ByteArrayInputStream(out.toByteArray());
    BloomFilter newBloomFilter = readFrom(numCandidates, in);
    assertEquals(bloomFilter, newBloomFilter);
  }

  @Test
  public void shouldMergeTwoFiltersCorrectly() {
    AdaptiveBloomFilter bloomFilter2 = new AdaptiveBloomFilter(numCandidates, 0.03);

    // Insert items into the first filter
    for (int i = 0; i < 1000; i++) {
      bloomFilter.put(i);
    }

    // Insert different items into the second filter
    for (int i = 1000; i < 2000; i++) {
      bloomFilter2.put(i);
    }

    // Merge the second filter into the first one
    bloomFilter.merge(bloomFilter2);

    // Check if the merged filter contains items from both filters
    for (int i = 0; i < 2000; i++) {
      assertTrue(bloomFilter.mightContain(i));
    }
    assertEquals(2048, bloomFilter.bestCandidate().expectedNumItems);
  }
}