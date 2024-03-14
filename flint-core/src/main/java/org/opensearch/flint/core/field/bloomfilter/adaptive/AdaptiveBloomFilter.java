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
import java.util.Iterator;
import java.util.Objects;
import java.util.function.Function;
import org.opensearch.flint.core.field.bloomfilter.BloomFilter;
import org.opensearch.flint.core.field.bloomfilter.classic.ClassicBloomFilter;

/**
 * Adaptive BloomFilter implementation that generates a series of bloom filter candidate
 * with different expected number of item (NDV) and at last choose the best one.
 */
public class AdaptiveBloomFilter implements BloomFilter {

  /**
   * Initial expected number of items for the first candidate.
   */
  public static final int INITIAL_EXPECTED_NUM_ITEMS = 1024;

  /**
   * Total number of distinct items seen so far.
   */
  private int cardinality = 0;

  /**
   * BloomFilter candidates.
   */
  private final BloomFilterCandidate[] candidates;

  /**
   * Construct adaptive BloomFilter instance with the given algorithm parameters.
   *
   * @param numCandidates number of candidate
   * @param fpp           false positive probability
   */
  public AdaptiveBloomFilter(int numCandidates, double fpp) {
    this.candidates = initializeCandidates(numCandidates,
        expectedNumItems -> new ClassicBloomFilter(expectedNumItems, fpp));
  }

  /**
   * Construct adaptive BloomFilter instance from BloomFilter array deserialized from input stream.
   *
   * @param cardinality total number of distinct items
   * @param candidates  BloomFilter candidates
   */
  AdaptiveBloomFilter(int cardinality, BloomFilter[] candidates) {
    this.cardinality = cardinality;
    Iterator<BloomFilter> it = Arrays.stream(candidates).iterator();
    this.candidates = initializeCandidates(candidates.length, expectedNumItems -> it.next());
  }

  /**
   * Deserialize adaptive BloomFilter instance from input stream.
   *
   * @param numCandidates number of candidates
   * @param in            input stream of serialized adaptive BloomFilter instance
   * @return adaptive BloomFilter instance
   */
  public static BloomFilter readFrom(int numCandidates, InputStream in) {
    try {
      // Read total distinct counter
      int cardinality = new DataInputStream(in).readInt();

      // Read BloomFilter candidate array
      BloomFilter[] candidates = new BloomFilter[numCandidates];
      for (int i = 0; i < numCandidates; i++) {
        candidates[i] = ClassicBloomFilter.readFrom(in);
      }
      return new AdaptiveBloomFilter(cardinality, candidates);
    } catch (IOException e) {
      throw new RuntimeException("Failed to deserialize adaptive BloomFilter", e);
    }
  }

  /**
   * @return best BloomFilter candidate which has expected number of item right above total distinct counter.
   */
  public BloomFilterCandidate bestCandidate() {
    return candidates[bestCandidateIndex()];
  }

  @Override
  public long bitSize() {
    return Arrays.stream(candidates)
        .mapToLong(c -> c.bloomFilter.bitSize())
        .sum();
  }

  @Override
  public boolean put(long item) {
    // Only insert into candidate with larger expectedNumItems for efficiency
    boolean bitChanged = false;
    for (int i = bestCandidateIndex(); i < candidates.length; i++) {
      bitChanged = candidates[i].bloomFilter.put(item);
    }

    // Use the last candidate's put result which is the most accurate
    if (bitChanged) {
      cardinality++;
    }
    return bitChanged;
  }

  @Override
  public BloomFilter merge(BloomFilter other) {
    AdaptiveBloomFilter otherBf = (AdaptiveBloomFilter) other;
    cardinality += otherBf.cardinality;

    for (int i = bestCandidateIndex(); i < candidates.length; i++) {
      candidates[i].bloomFilter.merge(otherBf.candidates[i].bloomFilter);
    }
    return this;
  }

  @Override
  public boolean mightContain(long item) {
    // Use the last candidate which is the most accurate
    return candidates[candidates.length - 1].bloomFilter.mightContain(item);
  }

  @Override
  public void writeTo(OutputStream out) throws IOException {
    // Serialized cardinality counter first
    new DataOutputStream(out).writeInt(cardinality);

    // Serialize classic BloomFilter array
    for (BloomFilterCandidate candidate : candidates) {
      candidate.bloomFilter.writeTo(out);
    }
  }

  private BloomFilterCandidate[] initializeCandidates(int numCandidates,
                                                      Function<Integer, BloomFilter> initializer) {
    BloomFilterCandidate[] candidates = new BloomFilterCandidate[numCandidates];
    int ndv = INITIAL_EXPECTED_NUM_ITEMS;

    // Initialize candidate with NDV doubled in each iteration
    for (int i = 0; i < numCandidates; i++, ndv *= 2) {
      candidates[i] = new BloomFilterCandidate(ndv, initializer.apply(ndv));
    }
    return candidates;
  }

  private int bestCandidateIndex() {
    int index = Arrays.binarySearch(candidates, new BloomFilterCandidate(cardinality, null));
    if (index < 0) {
      index = -(index + 1);
    }

    /*
     * Now 'index' represents the position where the current cardinality should be inserted,
     * indicating the best candidate to choose based on its expected number of distinct values.
     * The last one is chosen if cardinality exceeds each candidate's expected number.
     */
    return Math.min(index, candidates.length - 1);
  }

  /**
   * BloomFilter candidate that records expected number of items for each candidate.
   */
  public static class BloomFilterCandidate implements Comparable<BloomFilterCandidate> {
    /**
     * Expected number of items associated with this candidate.
     */
    private final int expectedNumItems;

    /**
     * BloomFilter instance.
     */
    private final BloomFilter bloomFilter;

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
    return cardinality == that.cardinality && Arrays.equals(candidates, that.candidates);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(cardinality);
    result = 31 * result + Arrays.hashCode(candidates);
    return result;
  }
}
