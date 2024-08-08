/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.metadata;

import java.lang.reflect.Constructor;
import org.apache.spark.SparkConf;
import org.opensearch.flint.common.metadata.FlintIndexMetadataService;
import org.opensearch.flint.core.FlintOptions;
import org.opensearch.flint.core.storage.FlintOpenSearchIndexMetadataService;

/**
 * {@link FlintIndexMetadataService} builder.
 * <p>
 * Custom implementations of {@link FlintIndexMetadataService} are expected to provide a public
 * constructor with the signature {@code public MyCustomService(SparkConf sparkConf)} to be
 * instantiated by this builder.
 */
public class FlintIndexMetadataServiceBuilder {
  public static FlintIndexMetadataService build(FlintOptions options, SparkConf sparkConf) {
    String className = options.getCustomFlintIndexMetadataServiceClass();
    if (className.isEmpty()) {
      return new FlintOpenSearchIndexMetadataService(options);
    }

    // Attempts to instantiate Flint index metadata service with sparkConf using reflection
    try {
      Class<?> flintIndexMetadataServiceClass = Class.forName(className);
      Constructor<?> constructor = flintIndexMetadataServiceClass.getConstructor(SparkConf.class);
      return (FlintIndexMetadataService) constructor.newInstance(sparkConf);
    } catch (Exception e) {
      throw new RuntimeException("Failed to instantiate FlintIndexMetadataService: " + className, e);
    }
  }
}