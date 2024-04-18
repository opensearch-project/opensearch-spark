/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.refresh

import org.opensearch.flint.spark.FlintSparkIndex
import org.opensearch.flint.spark.refresh.FlintSparkIndexRefresh.RefreshMode.RefreshMode

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.flint.config.FlintSparkConf

/**
 * Flint Spark index refresh that sync index data with source in style defined by concrete
 * implementation class.
 */
trait FlintSparkIndexRefresh extends Logging {

  /**
   * @return
   *   refresh mode
   */
  def refreshMode: RefreshMode

  /**
   * Validates the current index refresh settings before the actual execution begins. This method
   * checks for the integrity of the index refresh configurations and ensures that all options set
   * for the current refresh mode are valid. This preemptive validation helps in identifying
   * configuration issues before the refresh operation is initiated, minimizing runtime errors and
   * potential inconsistencies.
   *
   * @param spark
   *   Spark session
   * @throws IllegalArgumentException
   *   if any invalid or inapplicable config identified
   */
  def validate(spark: SparkSession): Unit

  /**
   * Start refreshing the index.
   *
   * @param spark
   *   Spark session to submit job
   * @param flintSparkConf
   *   Flint Spark configuration
   * @return
   *   optional Spark job ID
   */
  def start(spark: SparkSession, flintSparkConf: FlintSparkConf): Option[String]
}

object FlintSparkIndexRefresh {

  /** Index refresh mode */
  object RefreshMode extends Enumeration {
    type RefreshMode = Value
    val AUTO, FULL, INCREMENTAL = Value
  }

  /**
   * Create concrete index refresh implementation for the given index.
   *
   * @param indexName
   *   Flint index name
   * @param index
   *   Flint index
   * @return
   *   index refresh
   */
  def create(indexName: String, index: FlintSparkIndex): FlintSparkIndexRefresh = {
    val options = index.options
    if (options.autoRefresh()) {
      new AutoIndexRefresh(indexName, index)
    } else if (options.incrementalRefresh()) {
      new IncrementalIndexRefresh(indexName, index)
    } else {
      new FullIndexRefresh(indexName, index)
    }
  }
}
