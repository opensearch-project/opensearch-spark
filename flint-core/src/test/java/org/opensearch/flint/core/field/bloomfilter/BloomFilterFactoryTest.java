/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.field.bloomfilter;

import static java.util.Collections.emptyMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.opensearch.flint.core.field.bloomfilter.BloomFilterFactory.ADAPTIVE_NUMBER_CANDIDATE_KEY;
import static org.opensearch.flint.core.field.bloomfilter.BloomFilterFactory.BLOOM_FILTER_ADAPTIVE_KEY;
import static org.opensearch.flint.core.field.bloomfilter.BloomFilterFactory.CLASSIC_BLOOM_FILTER_FPP_KEY;
import static org.opensearch.flint.core.field.bloomfilter.BloomFilterFactory.CLASSIC_BLOOM_FILTER_NUM_ITEMS_KEY;
import static org.opensearch.flint.core.field.bloomfilter.BloomFilterFactory.DEFAULT_ADAPTIVE_NUMBER_CANDIDATE;
import static org.opensearch.flint.core.field.bloomfilter.BloomFilterFactory.DEFAULT_CLASSIC_BLOOM_FILTER_FPP;
import static org.opensearch.flint.core.field.bloomfilter.BloomFilterFactory.DEFAULT_CLASSIC_BLOOM_FILTER_NUM_ITEMS;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import org.junit.Test;
import org.opensearch.flint.core.field.bloomfilter.adaptive.AdaptiveBloomFilter;
import org.opensearch.flint.core.field.bloomfilter.classic.ClassicBloomFilter;

public class BloomFilterFactoryTest {

  @Test
  public void createAdaptiveBloomFilter() {
    BloomFilterFactory factory = BloomFilterFactory.of(emptyMap());

    assertInstanceOf(AdaptiveBloomFilter.class, factory.create());
    assertEquals(
        Map.of(
            BLOOM_FILTER_ADAPTIVE_KEY, "true",
            ADAPTIVE_NUMBER_CANDIDATE_KEY, Integer.toString(DEFAULT_ADAPTIVE_NUMBER_CANDIDATE),
            CLASSIC_BLOOM_FILTER_FPP_KEY, Double.toString(DEFAULT_CLASSIC_BLOOM_FILTER_FPP)),
        factory.getParameters());
  }

  @Test
  public void createAdaptiveBloomFilterWithParameters() {
    Map<String, String> parameters =
        Map.of(
            BLOOM_FILTER_ADAPTIVE_KEY, "true",
            ADAPTIVE_NUMBER_CANDIDATE_KEY, "8",
            CLASSIC_BLOOM_FILTER_FPP_KEY, "0.01");
    BloomFilterFactory factory = BloomFilterFactory.of(parameters);

    assertInstanceOf(AdaptiveBloomFilter.class, factory.create());
    assertEquals(parameters, factory.getParameters());
  }

  @Test
  public void shouldCreateClassicBloomFilter() {
    BloomFilterFactory factory = BloomFilterFactory.of(Map.of(BLOOM_FILTER_ADAPTIVE_KEY, "false"));

    assertEquals(
        Map.of(
            BLOOM_FILTER_ADAPTIVE_KEY, "false",
            CLASSIC_BLOOM_FILTER_NUM_ITEMS_KEY, Integer.toString(DEFAULT_CLASSIC_BLOOM_FILTER_NUM_ITEMS),
            CLASSIC_BLOOM_FILTER_FPP_KEY, Double.toString(DEFAULT_CLASSIC_BLOOM_FILTER_FPP)),
        factory.getParameters());
    assertInstanceOf(ClassicBloomFilter.class, factory.create());
  }

  @Test
  public void shouldCreateClassicBloomFilterWithParameters() {
    Map<String, String> parameters =
        Map.of(
            BLOOM_FILTER_ADAPTIVE_KEY, "false",
            CLASSIC_BLOOM_FILTER_NUM_ITEMS_KEY, "20000",
            CLASSIC_BLOOM_FILTER_FPP_KEY, "0.02");
    BloomFilterFactory factory = BloomFilterFactory.of(parameters);

    assertInstanceOf(ClassicBloomFilter.class, factory.create());
    assertEquals(parameters, factory.getParameters());
  }

  @Test
  public void deserializeAdaptiveBloomFilter() throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    new AdaptiveBloomFilter(
        DEFAULT_ADAPTIVE_NUMBER_CANDIDATE,
        DEFAULT_CLASSIC_BLOOM_FILTER_FPP
    ).writeTo(out);
    ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());

    BloomFilterFactory factory = BloomFilterFactory.of(emptyMap());
    assertInstanceOf(AdaptiveBloomFilter.class, factory.deserialize(in));
  }

  @Test
  public void deserializeClassicBloomFilter() throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    new ClassicBloomFilter(
        DEFAULT_CLASSIC_BLOOM_FILTER_NUM_ITEMS,
        DEFAULT_CLASSIC_BLOOM_FILTER_FPP
    ).writeTo(out);
    ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());

    BloomFilterFactory factory = BloomFilterFactory.of(Map.of(BLOOM_FILTER_ADAPTIVE_KEY, "false"));
    assertInstanceOf(ClassicBloomFilter.class, factory.deserialize(in));
  }
}