/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.metadata;

import java.lang.reflect.Constructor;
import org.opensearch.flint.common.metadata.FlintIndexMetadataService;
import org.opensearch.flint.core.FlintOptions;
import org.opensearch.flint.core.storage.FlintOpenSearchIndexMetadataService;

/**
 * {@link FlintIndexMetadataService} builder.
 * <p>
 * Custom implementations of {@link FlintIndexMetadataService} are expected to provide a public
 * constructor with no arguments to be instantiated by this builder.
 */
public class FlintIndexMetadataServiceBuilder {
  public static FlintIndexMetadataService build(FlintOptions options) {
    String className = options.getCustomFlintIndexMetadataServiceClass();
    if (className.isEmpty()) {
      return new FlintOpenSearchIndexMetadataService(options);
    }

    // Attempts to instantiate Flint index metadata service using reflection
    try {
      Class<?> flintIndexMetadataServiceClass = Class.forName(className);
      Constructor<?> constructor = flintIndexMetadataServiceClass.getConstructor();
      return (FlintIndexMetadataService) constructor.newInstance();
    } catch (Exception e) {
      throw new RuntimeException("Failed to instantiate FlintIndexMetadataService: " + className, e);
    }
  }
}
