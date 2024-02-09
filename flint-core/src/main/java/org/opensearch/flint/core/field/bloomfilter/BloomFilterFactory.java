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
   * Bloom filter algorithm parameters.
   */
  private final Map<String, String> parameters;

  public static BloomFilterFactory from(Map<String, String> parameters) {
    if (isAdaptiveEnabled(parameters)) {
      return new AdaptiveBloomFilterFactory(parameters);
    } else {
      return new ClassicBloomFilterFactory(parameters);
    }
  }

  protected BloomFilterFactory(Map<String, String> parameters) {
    this.parameters = parameters;
  }

  public abstract Map<String, String> getParameters();

  public abstract BloomFilter create();

  public abstract BloomFilter deserialize(InputStream in);

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

  static class ClassicBloomFilterFactory extends BloomFilterFactory {

    protected ClassicBloomFilterFactory(Map<String, String> parameters) {
      super(parameters);
    }

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
  }

  static class AdaptiveBloomFilterFactory extends BloomFilterFactory {

    protected AdaptiveBloomFilterFactory(Map<String, String> parameters) {
      super(parameters);
    }

    @Override
    public Map<String, String> getParameters() {
      return Map.of(
          BLOOM_FILTER_ADAPTIVE_KEY, "true",
          CLASSIC_BLOOM_FILTER_FPP_KEY, Double.toString(fpp()));
    }

    @Override
    public BloomFilter create() {
      return new AdaptiveBloomFilter(fpp());
    }

    @Override
    public BloomFilter deserialize(InputStream in) {
      return AdaptiveBloomFilter.readFrom(in);
    }
  }
}
