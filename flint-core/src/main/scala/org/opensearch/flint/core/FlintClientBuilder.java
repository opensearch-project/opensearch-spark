/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core;

import java.lang.reflect.Constructor;
import org.opensearch.flint.core.storage.FlintOpenSearchClient;
import org.opensearch.flint.core.metadata.log.FlintMetadataLogService;

/**
 * {@link FlintClient} builder.
 */
public class FlintClientBuilder {

  public static FlintClient build(FlintOptions options) {
    return new FlintOpenSearchClient(options,
        instantiateFlintMetadataLogService(options.getCustomFlintMetadataLogServiceClass(), options));
  }

  /**
   * Attempts to instantiate Flint metadata log service using reflection.
   */
  private static FlintMetadataLogService instantiateFlintMetadataLogService(String className, FlintOptions options) {
    try {
      Class<?> flintMetadataLogServiceClass = Class.forName(className);
      Constructor<?> constructor = flintMetadataLogServiceClass.getConstructor(FlintOptions.class);
      return (FlintMetadataLogService) constructor.newInstance(options);
    } catch (Exception e) {
      throw new RuntimeException("Failed to instantiate FlintMetadataLogService: " + className, e);
    }
  }
}
