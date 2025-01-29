/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.spark.e2e

import org.apache.spark.sql.SparkSession

trait SparkTrait {
  var spark : SparkSession = null

  def getSparkConnectPort(): Int

  def getSparkSession(): SparkSession = {
    this.synchronized {
      if (spark == null) {
        spark = SparkSession.builder().remote("sc://localhost:" + getSparkConnectPort()).build()
      }
      spark
    }
  }
}
