/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.iceberg

import org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
import org.opensearch.flint.spark.FlintSparkExtensions
import org.opensearch.flint.spark.FlintSparkSuite

import org.apache.spark.SparkConf

/**
 * Flint Spark suite tailored for Iceberg.
 */
trait FlintSparkIcebergSuite extends FlintSparkSuite {

  // Override table type to Iceberg for this suite
  override lazy protected val tableType: String = "iceberg"

  // You can also override tableOptions if Iceberg requires different options
  override lazy protected val tableOptions: String = ""

  // Override the sparkConf method to include Iceberg-specific configurations
  override protected def sparkConf: SparkConf = {
    val conf = super.sparkConf
      // Set Iceberg-specific Spark configurations
      .set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
      .set("spark.sql.catalog.spark_catalog.type", "hadoop")
      .set("spark.sql.catalog.spark_catalog.warehouse", s"spark-warehouse/${suiteName}")
      .set(
        "spark.sql.extensions",
        List(
          classOf[IcebergSparkSessionExtensions].getName,
          classOf[FlintSparkExtensions].getName).mkString(", "))
    conf
  }

  override def afterAll(): Unit = {
    deleteDirectory(s"spark-warehouse/${suiteName}")
    super.afterAll()
  }

}
