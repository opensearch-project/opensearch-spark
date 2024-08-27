/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.metadata.log;

import java.lang.reflect.Constructor;
import org.opensearch.flint.common.metadata.log.FlintMetadataLogService;
import org.opensearch.flint.core.FlintOptions;
import org.opensearch.flint.core.storage.FlintOpenSearchMetadataLogService;

/**
 * {@link FlintMetadataLogService} builder.
 * <p>
 * Custom implementations of {@link FlintMetadataLogService} are expected to provide a public
 * constructor with no arguments to be instantiated by this builder.
 */
public class FlintMetadataLogServiceBuilder {
  public static FlintMetadataLogService build(FlintOptions options) {
    String className = options.getCustomFlintMetadataLogServiceClass();
    if (className.isEmpty()) {
      return new FlintOpenSearchMetadataLogService(options);
    }

    // Attempts to instantiate Flint metadata log service using reflection
    try {
      Class<?> flintMetadataLogServiceClass = Class.forName(className);
      Constructor<?> constructor = flintMetadataLogServiceClass.getConstructor();
      return (FlintMetadataLogService) constructor.newInstance();
    } catch (Exception e) {
      throw new RuntimeException("Failed to instantiate FlintMetadataLogService: " + className, e);
    }
  }
}
