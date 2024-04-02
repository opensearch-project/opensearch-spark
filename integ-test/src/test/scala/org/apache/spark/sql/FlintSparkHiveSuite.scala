/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql

import org.opensearch.flint.spark.FlintSparkSuite

import org.apache.spark.SparkConf
import org.apache.spark.sql.hive.HiveSessionStateBuilder
import org.apache.spark.sql.internal.{SessionState, StaticSQLConf}
import org.apache.spark.sql.test.TestSparkSession

/**
 * Flint Spark base suite with Hive support enabled. Because enabling Hive support in Spark
 * configuration alone is not adequate, as [[TestSparkSession]] disregards it and consistently
 * creates its own instance of [[org.apache.spark.sql.test.TestSQLSessionStateBuilder]]. We need
 * to override its session state with that of Hive in the meanwhile.
 */
trait FlintSparkHiveSuite extends FlintSparkSuite {

  override protected def sparkConf: SparkConf = {
    // Enable Hive support
    val conf =
      super.sparkConf
        .set(StaticSQLConf.CATALOG_IMPLEMENTATION.key, "hive")
    conf
  }

  override protected def createSparkSession: TestSparkSession = {
    SparkSession.cleanupAnyExistingSession()
    new FlintTestSparkSession(sparkConf)
  }

  class FlintTestSparkSession(sparkConf: SparkConf) extends TestSparkSession(sparkConf) { self =>

    override lazy val sessionState: SessionState = {
      // Override to replace [[TestSQLSessionStateBuilder]] with Hive session state
      new HiveSessionStateBuilder(spark, None).build()
    }
  }
}
