/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.field.bloomfilter;

import java.io.InputStream;
import java.io.Serializable;
import java.util.Map;
import java.util.Optional;
import org.opensearch.flint.core.field.bloomfilter.adaptive.AdaptiveBloomFilter;
import org.opensearch.flint.core.field.bloomfilter.classic.ClassicBloomFilter;

/**
 * Bloom filter factory that builds bloom filter based on algorithm parameters.
 */
public abstract class BloomFilterFactory implements Serializable {

  /**
   * Bloom filter adaptive key and default value.
   */
  public static final String BLOOM_FILTER_ADAPTIVE_KEY = "adaptive";
  public static final boolean DEFAULT_BLOOM_FILTER_ADAPTIVE = true;

  /**
   * Expected number of unique items key and default value.
   */
  public static final String CLASSIC_BLOOM_FILTER_NUM_ITEMS_KEY = "num_items";
  public static final int DEFAULT_CLASSIC_BLOOM_FILTER_NUM_ITEMS = 10000;

  /**
   * False positive probability (FPP) key and default value.
   */
  public static final String CLASSIC_BLOOM_FILTER_FPP_KEY = "fpp";
  public static final double DEFAULT_CLASSIC_BLOOM_FILTER_FPP = 0.03;

  /**
   * Number of candidate key and default value.
   */
  public static final String ADAPTIVE_NUMBER_CANDIDATE_KEY = "num_candidates";
  public static final int DEFAULT_ADAPTIVE_NUMBER_CANDIDATE = 10;

  /**
   * Bloom filter algorithm parameters.
   */
  private final Map<String, String> parameters;

  protected BloomFilterFactory(Map<String, String> parameters) {
    this.parameters = parameters;
  }

  /**
   * @return all parameters including the default ones.
   */
  public abstract Map<String, String> getParameters();

  /**
   * Create specific BloomFilter instance.
   *
   * @return BloomFilter instance
   */
  public abstract BloomFilter create();

  /**
   * Create specific BloomFilter instance by deserialization.
   *
   * @param in input stream
   * @return BloomFilter instance
   */
  public abstract BloomFilter deserialize(InputStream in);

  /**
   * Create specific BloomFilter factory given the parameters.
   *
   * @param parameters BloomFilter parameters
   * @return BloomFilter factory instance
   */
  public static BloomFilterFactory of(Map<String, String> parameters) {
    if (isAdaptiveEnabled(parameters)) {
      return createAdaptiveBloomFilterFactory(parameters);
    } else {
      return createClassicBloomFilterFactory(parameters);
    }
  }

  private static BloomFilterFactory createAdaptiveBloomFilterFactory(Map<String, String> parameters) {
    return new BloomFilterFactory(parameters) {
      @Override
      public Map<String, String> getParameters() {
        return Map.of(
            BLOOM_FILTER_ADAPTIVE_KEY, "true",
            ADAPTIVE_NUMBER_CANDIDATE_KEY, Integer.toString(numCandidates()),
            CLASSIC_BLOOM_FILTER_FPP_KEY, Double.toString(fpp()));
      }

      @Override
      public BloomFilter create() {
        return new AdaptiveBloomFilter(numCandidates(), fpp());
      }

      @Override
      public BloomFilter deserialize(InputStream in) {
        return AdaptiveBloomFilter.readFrom(numCandidates(), in);
      }
    };
  }

  private static BloomFilterFactory createClassicBloomFilterFactory(Map<String, String> parameters) {
    return new BloomFilterFactory(parameters) {
      @Override
      public Map<String, String> getParameters() {
        return Map.of(
            BLOOM_FILTER_ADAPTIVE_KEY, "false",
            CLASSIC_BLOOM_FILTER_NUM_ITEMS_KEY, Integer.toString(expectedNumItems()),
            CLASSIC_BLOOM_FILTER_FPP_KEY, Double.toString(fpp()));
      }

      @Override
      public BloomFilter create() {
        return new ClassicBloomFilter(expectedNumItems(), fpp());
      }

      @Override
      public BloomFilter deserialize(InputStream in) {
        return ClassicBloomFilter.readFrom(in);
      }
    };
  }

  private static boolean isAdaptiveEnabled(Map<String, String> params) {
    return Optional.ofNullable(params.get(BLOOM_FILTER_ADAPTIVE_KEY))
        .map(Boolean::parseBoolean)
        .orElse(DEFAULT_BLOOM_FILTER_ADAPTIVE);
  }

  protected int expectedNumItems() {
    return Optional.ofNullable(parameters.get(CLASSIC_BLOOM_FILTER_NUM_ITEMS_KEY))
        .map(Integer::parseInt)
        .orElse(DEFAULT_CLASSIC_BLOOM_FILTER_NUM_ITEMS);
  }

  protected double fpp() {
    return Optional.ofNullable(parameters.get(CLASSIC_BLOOM_FILTER_FPP_KEY))
        .map(Double::parseDouble)
        .orElse(DEFAULT_CLASSIC_BLOOM_FILTER_FPP);
  }

  protected int numCandidates() {
    return Optional.ofNullable(parameters.get(ADAPTIVE_NUMBER_CANDIDATE_KEY))
        .map(Integer::parseInt)
        .orElse(DEFAULT_ADAPTIVE_NUMBER_CANDIDATE);
  }
}
