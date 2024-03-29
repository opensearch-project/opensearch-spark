/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.hive.HiveSessionStateBuilder
import org.apache.spark.sql.internal.SessionState
import org.apache.spark.sql.test.TestSparkSession

class FlintTestSparkSession(sparkConf: SparkConf) extends TestSparkSession(sparkConf) {

  override lazy val sessionState: SessionState = {
    new HiveSessionStateBuilder(this, None).build()
  }
}
