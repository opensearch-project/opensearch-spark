/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.metadata.log;

import java.lang.reflect.Constructor;
import java.util.logging.Logger;
import org.apache.spark.SparkConf;
import org.opensearch.flint.common.metadata.log.FlintMetadataLogService;
import org.opensearch.flint.core.FlintOptions;
import org.opensearch.flint.core.storage.FlintOpenSearchMetadataLogService;

/**
 * {@link FlintMetadataLogService} builder.
 * <p>
 * Custom implementations of {@link FlintMetadataLogService} are expected to provide a public
 * constructor with the signature {@code public MyCustomService(SparkConf sparkConf)} to be
 * instantiated by this builder.
 */
public class FlintMetadataLogServiceBuilder {

  private static final Logger LOG = Logger.getLogger(FlintMetadataLogServiceBuilder.class.getName());

  public static FlintMetadataLogService build(FlintOptions options, SparkConf sparkConf) {
    String className = options.getCustomFlintMetadataLogServiceClass();
    LOG.info("Building flint metadata log service. Class name: " + className);
    if (className.isEmpty()) {
      LOG.info("Building flint OpenSearch metadata log service.");
      return new FlintOpenSearchMetadataLogService(options);
    }

    // Attempts to instantiate Flint metadata log service with sparkConf using reflection
    try {
      LOG.info("Building flint custom metadata log service with " + className);
      Class<?> flintMetadataLogServiceClass = Class.forName(className);
      Constructor<?> constructor = flintMetadataLogServiceClass.getConstructor(SparkConf.class);
      return (FlintMetadataLogService) constructor.newInstance(sparkConf);
    } catch (Exception e) {
      throw new RuntimeException("Failed to instantiate FlintMetadataLogService: " + className, e);
    }
  }
}
