/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.scheduler;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.flint.config.FlintSparkConf;
import org.opensearch.flint.common.scheduler.AsyncQueryScheduler;
import org.opensearch.flint.core.FlintOptions;

import java.io.IOException;
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

  public static AsyncQueryScheduler build(SparkSession sparkSession, FlintOptions options) throws IOException {
    return new AsyncQuerySchedulerBuilder().doBuild(sparkSession, options);
  }

  /**
   * Builds an AsyncQueryScheduler based on the provided options.
   *
   * @param sparkSession The SparkSession to be used.
   * @param options The FlintOptions containing configuration details.
   * @return An instance of AsyncQueryScheduler.
   */
  protected AsyncQueryScheduler doBuild(SparkSession sparkSession, FlintOptions options) throws IOException {
    String className = options.getCustomAsyncQuerySchedulerClass();

    if (className.isEmpty()) {
      OpenSearchAsyncQueryScheduler scheduler = createOpenSearchAsyncQueryScheduler(options);
      // Check if the scheduler has access to the required index. Disable the external scheduler otherwise.
      if (!hasAccessToSchedulerIndex(scheduler)){
        setExternalSchedulerEnabled(sparkSession, false);
      }
      return scheduler;
    }

    // Attempts to instantiate AsyncQueryScheduler using reflection
    logger.info("Attempting to instantiate AsyncQueryScheduler with class name: {}", className);
    try {
      Class<?> asyncQuerySchedulerClass = Class.forName(className);
      Constructor<?> constructor = asyncQuerySchedulerClass.getConstructor();
      return (AsyncQueryScheduler) constructor.newInstance();
    } catch (Exception e) {
      throw new RuntimeException("Failed to instantiate AsyncQueryScheduler: " + className, e);
    }
  }

  protected OpenSearchAsyncQueryScheduler createOpenSearchAsyncQueryScheduler(FlintOptions options) {
    return new OpenSearchAsyncQueryScheduler(options);
  }

  protected boolean hasAccessToSchedulerIndex(OpenSearchAsyncQueryScheduler scheduler) throws IOException {
    return scheduler.hasAccessToSchedulerIndex();
  }

  protected void setExternalSchedulerEnabled(SparkSession sparkSession, boolean enabled) {
    sparkSession.sqlContext().setConf(FlintSparkConf.EXTERNAL_SCHEDULER_ENABLED().key(), String.valueOf(enabled));
  }
}