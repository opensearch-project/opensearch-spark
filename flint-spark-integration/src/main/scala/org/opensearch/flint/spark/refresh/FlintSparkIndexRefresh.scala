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
   * Validate the current index refresh beforehand.
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
