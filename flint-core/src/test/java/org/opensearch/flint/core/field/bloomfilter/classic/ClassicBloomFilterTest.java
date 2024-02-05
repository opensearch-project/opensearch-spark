/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.field.bloomfilter.classic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import org.junit.Test;
import org.opensearch.flint.core.field.bloomfilter.BloomFilter;

public class ClassicBloomFilterTest {

  private final ClassicBloomFilter bloomFilter = new ClassicBloomFilter(100, 0.01);

  private static final double ACCEPTABLE_FALSE_POSITIVE_RATE = 0.2;

  @Test
  public void shouldReturnNoFalseNegative() {
    bloomFilter.put(123L);
    bloomFilter.put(456L);
    bloomFilter.put(789L);

    // For items added, expect no false negative
    assertTrue(bloomFilter.mightContain(123L));
    assertTrue(bloomFilter.mightContain(456L));
    assertTrue(bloomFilter.mightContain(789L));
  }

  @Test
  public void shouldReturnFalsePositiveLessThanConfigured() {
    bloomFilter.put(123L);
    bloomFilter.put(456L);
    bloomFilter.put(789L);

    // For items not added, expect false positives much lower than configure 1%
    int numElements = 1000;
    int falsePositiveCount = 0;
    for (int i = 0; i < numElements; i++) {
      long element = 1000L + i;
      if (bloomFilter.mightContain(element)) {
        falsePositiveCount++;
      }
    }

    double actualFalsePositiveRate = (double) falsePositiveCount / numElements;
    assertTrue(actualFalsePositiveRate <= ACCEPTABLE_FALSE_POSITIVE_RATE,
        "Actual false positive rate is higher than expected");
  }

  @Test
  public void shouldBeTheSameAfterWriteToAndReadFrom() throws IOException {
    bloomFilter.put(123L);
    bloomFilter.put(456L);
    bloomFilter.put(789L);

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    bloomFilter.writeTo(out);
    InputStream in = new ByteArrayInputStream(out.toByteArray());
    BloomFilter newBloomFilter = ClassicBloomFilter.readFrom(in);
    assertEquals(bloomFilter, newBloomFilter);
  }
}