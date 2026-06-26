/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

class FlintOptionsTest {

  @Test
  public void getInactivityLimitMillisReturnsDefaultWhenNotConfigured() {
    FlintOptions options = new FlintOptions(ImmutableMap.of());
    assertEquals(FlintOptions.DEFAULT_INACTIVITY_LIMIT_MILLIS, options.getInactivityLimitMillis());
  }

  @Test
  public void getInactivityLimitMillisReturnsConfiguredValue() {
    FlintOptions options =
        new FlintOptions(ImmutableMap.of(FlintOptions.INACTIVITY_LIMIT_MILLIS, "30000"));
    assertEquals(30000, options.getInactivityLimitMillis());
  }
}
