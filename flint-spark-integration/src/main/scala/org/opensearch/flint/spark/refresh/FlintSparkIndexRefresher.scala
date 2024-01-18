/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.refresh

import org.opensearch.flint.spark.FlintSparkIndex

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.flint.config.FlintSparkConf

/**
 * Flint Spark index refresher that validates and starts the index refresh.
 */
trait FlintSparkIndexRefresher extends Logging {

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

object FlintSparkIndexRefresher {

  def create(indexName: String, index: FlintSparkIndex): FlintSparkIndexRefresher = {
    if (index.options.autoRefresh()) {
      new AutoIndexRefresher(indexName, index)
    } else {
      new FullManualIndexRefresher(indexName, index)
    }
  }
}
