/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.spark.e2e

import org.apache.spark.sql.SparkSession

trait SparkTrait {
  var spark : SparkSession = null

  /**
   * Retrieves the exposed port of Spark Connect on the "spark" container.
   *
   * @return Spark Connect port of the "spark" container
   */
  def getSparkConnectPort(): Int

  /**
   * Returns an SparkSession object. Constructs a new SparkSession object for use with the integration test docker
   * cluster "spark" container. Creates a new SparkSession first time this is called, otherwise the existing S3
   * client is returned.
   *
   * @return a SparkSession for use with the integration test docker cluster
   */
  def getSparkSession(): SparkSession = {
    this.synchronized {
      if (spark == null) {
        spark = SparkSession.builder().remote("sc://localhost:" + getSparkConnectPort()).build()
      }
      spark
    }
  }
}
