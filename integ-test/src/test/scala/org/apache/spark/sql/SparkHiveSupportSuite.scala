/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.hive.HiveSessionStateBuilder
import org.apache.spark.sql.internal.{SessionState, StaticSQLConf}
import org.apache.spark.sql.test.{SharedSparkSession, TestSparkSession}

/**
 * Flint Spark base suite with Hive support enabled. Because enabling Hive support in Spark
 * configuration alone is not adequate, as [[TestSparkSession]] disregards it and consistently
 * creates its own instance of [[org.apache.spark.sql.test.TestSQLSessionStateBuilder]]. We need
 * to override its session state with that of Hive in the meanwhile.
 *
 * Note that we need to extend [[SharedSparkSession]] to call super.sparkConf() method.
 */
trait SparkHiveSupportSuite extends SharedSparkSession {

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      // Enable Hive support
      .set(StaticSQLConf.CATALOG_IMPLEMENTATION.key, "hive")
      // Use in-memory Derby as Hive metastore so no need to clean up metastore_db folder after test
      .set("javax.jdo.option.ConnectionURL", "jdbc:derby:memory:metastore_db;create=true")
      .set("hive.metastore.uris", "")
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
