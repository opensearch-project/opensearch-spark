/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.field.bloomfilter.adaptive;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import org.opensearch.flint.core.field.bloomfilter.BloomFilter;
import org.opensearch.flint.core.field.bloomfilter.classic.ClassicBloomFilter;

public class AdaptiveBloomFilter implements BloomFilter {

  private int total = 0;

  private static final int K = 1024;
  private final int[] ranges = {
      128, 256, 512, K, 2 * K, 4 * K, 8 * K, 16 * K, 32 * K, 64 * K, 128 * K, 256 * K
  };
  // Expose for UT
  final BloomFilter[] candidates;

  public AdaptiveBloomFilter(double fpp) {
    this.candidates = new BloomFilter[ranges.length];
    for (int i = 0; i < candidates.length; i++) {
      candidates[i] = new ClassicBloomFilter(ranges[i], fpp);
    }
  }

  AdaptiveBloomFilter(BloomFilter[] candidates) {
    this.candidates = candidates;
  }

  @Override
  public long bitSize() {
    return Arrays.stream(candidates).map(BloomFilter::bitSize).count();
  }

  @Override
  public boolean put(long item) {
    // Use last result
    boolean bitChanged = false;
    for (int i = bestCandidateIndex(); i < candidates.length; i++) {
      bitChanged = candidates[i].put(item);
    }

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
      candidates[i].merge(otherBf.candidates[i]);
    }
    return null;
  }

  @Override
  public boolean mightContain(long item) {
    return candidates[candidates.length - 1].mightContain(item);
  }

  @Override
  public void writeTo(OutputStream out) throws IOException {
    for (BloomFilter candidate : candidates) {
      candidate.writeTo(out);
    }
  }

  public static BloomFilter readFrom(InputStream in) {
    BloomFilter[] candidates = new BloomFilter[12];
    for (int i = 0; i < 12; i++) {
      candidates[i] = ClassicBloomFilter.readFrom(in);
    }
    return new AdaptiveBloomFilter(candidates);
  }

  public BloomFilter bestCandidate() {
    return candidates[bestCandidateIndex()];
  }

  private int bestCandidateIndex() {
    int index = Arrays.binarySearch(ranges, total);
    if (index < 0) {
      index = -(index + 1);
    }
    return Math.min(index, candidates.length);
  }
}
