/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.field.bloomfilter.adaptive;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Objects;
import org.opensearch.flint.core.field.bloomfilter.BloomFilter;
import org.opensearch.flint.core.field.bloomfilter.classic.ClassicBloomFilter;

/**
 * Adaptive bloom filter implementation that generates a series of bloom filter candidate
 * with different expected number of item (NDV) and at last choose the best one.
 */
public class AdaptiveBloomFilter implements BloomFilter {

  /**
   * Total number of distinct items seen so far.
   */
  private int total = 0;

  /**
   * Bloom filter candidates.
   */
  final BloomFilterCandidate[] candidates;

  public AdaptiveBloomFilter(int numCandidate, double fpp) {
    this.candidates = new BloomFilterCandidate[numCandidate];

    int expectedNumItems = 1024;
    for (int i = 0; i < candidates.length; i++) {
      candidates[i] =
          new BloomFilterCandidate(
              expectedNumItems,
              new ClassicBloomFilter(expectedNumItems, fpp));
      expectedNumItems *= 2;
    }
  }

  AdaptiveBloomFilter(int total, BloomFilter[] candidates) {
    this.total = total;
    this.candidates = new BloomFilterCandidate[candidates.length];

    int expectedNumItems = 1024;
    for (int i = 0; i < candidates.length; i++) {
      this.candidates[i] = new BloomFilterCandidate(expectedNumItems, candidates[i]);
      expectedNumItems *= 2;
    }
  }

  @Override
  public long bitSize() {
    return Arrays.stream(candidates).map(candidate -> candidate.bloomFilter.bitSize()).count();
  }

  @Override
  public boolean put(long item) {
    // Only insert into candidate with larger expectedNumItems for efficiency
    boolean bitChanged = false;
    for (int i = bestCandidateIndex(); i < candidates.length; i++) {
      bitChanged = candidates[i].bloomFilter.put(item);
    }

    // Use the last candidate's put result which is most accurate
    if (bitChanged) {
      total++;
    }
    return bitChanged;
  }

  @Override
  public BloomFilter merge(BloomFilter other) {
    AdaptiveBloomFilter otherBf = (AdaptiveBloomFilter) other;
    total += otherBf.total;

    for (int i = 0; i < candidates.length; i++) {
      candidates[i].bloomFilter.merge(otherBf.candidates[i].bloomFilter);
    }
    return null;
  }

  @Override
  public boolean mightContain(long item) {
    return candidates[candidates.length - 1].bloomFilter.mightContain(item);
  }

  @Override
  public void writeTo(OutputStream out) throws IOException {
    new DataOutputStream(out).writeInt(total);
    for (BloomFilterCandidate candidate : candidates) {
      candidate.bloomFilter.writeTo(out);
    }
  }

  public static BloomFilter readFrom(int numCandidates, InputStream in) {
    try {
      int total = new DataInputStream(in).readInt();
      BloomFilter[] candidates = new BloomFilter[numCandidates];
      for (int i = 0; i < numCandidates; i++) {
        candidates[i] = ClassicBloomFilter.readFrom(in);
      }
      return new AdaptiveBloomFilter(total, candidates);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public BloomFilterCandidate bestCandidate() {
    return candidates[bestCandidateIndex()];
  }

  private int bestCandidateIndex() {
    int index = Arrays.binarySearch(candidates, new BloomFilterCandidate(total, null));
    if (index < 0) {
      index = -(index + 1);
    }
    return Math.min(index, candidates.length - 1);
  }

  public static class BloomFilterCandidate implements Comparable<BloomFilterCandidate> {

    int expectedNumItems;
    BloomFilter bloomFilter;

    BloomFilterCandidate(int expectedNumItems, BloomFilter bloomFilter) {
      this.expectedNumItems = expectedNumItems;
      this.bloomFilter = bloomFilter;
    }

    public int getExpectedNumItems() {
      return expectedNumItems;
    }

    public BloomFilter getBloomFilter() {
      return bloomFilter;
    }

    @Override
    public int compareTo(BloomFilterCandidate other) {
      return Integer.compare(expectedNumItems, other.expectedNumItems);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      BloomFilterCandidate that = (BloomFilterCandidate) o;
      return expectedNumItems == that.expectedNumItems && Objects.equals(bloomFilter, that.bloomFilter);
    }

    @Override
    public int hashCode() {
      return Objects.hash(expectedNumItems, bloomFilter);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    AdaptiveBloomFilter that = (AdaptiveBloomFilter) o;
    return total == that.total && Arrays.equals(candidates, that.candidates);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(total);
    result = 31 * result + Arrays.hashCode(candidates);
    return result;
  }
}
