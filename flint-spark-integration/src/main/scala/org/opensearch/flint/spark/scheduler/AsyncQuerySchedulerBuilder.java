/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.scheduler;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.flint.common.scheduler.AsyncQueryScheduler;
import org.opensearch.flint.core.FlintOptions;

import java.lang.reflect.Constructor;

/**
 * {@link AsyncQueryScheduler} builder.
 * <p>
 * Custom implementations of {@link AsyncQueryScheduler} are expected to provide a public
 * constructor with no arguments to be instantiated by this builder.
 */
public class AsyncQuerySchedulerBuilder {
  private static final Logger logger = LogManager.getLogger(AsyncQuerySchedulerBuilder.class);

  public enum AsyncQuerySchedulerAction {
    SCHEDULE,
    UPDATE,
    UNSCHEDULE,
    REMOVE
  }

  public static AsyncQueryScheduler build(FlintOptions options) {
    String className = options.getCustomAsyncQuerySchedulerClass();
    logger.info("Attempting to instantiate AsyncQueryScheduler with class name: {}", className);

    if (className.isEmpty()) {
      return new OpenSearchAsyncQueryScheduler(options);
    }

    // Attempts to instantiate AsyncQueryScheduler using reflection
    try {
      Class<?> asyncQuerySchedulerClass = Class.forName(className);
      Constructor<?> constructor = asyncQuerySchedulerClass.getConstructor();
      return (AsyncQueryScheduler) constructor.newInstance();
    } catch (Exception e) {
      throw new RuntimeException("Failed to instantiate AsyncQueryScheduler: " + className, e);
    }
  }
}