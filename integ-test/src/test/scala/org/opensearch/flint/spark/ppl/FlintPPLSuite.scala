/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
import org.opensearch.flint.spark.{FlintPPLSparkExtensions, FlintSparkExtensions, FlintSparkSuite}

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions.CodegenObjectFactoryMode
import org.apache.spark.sql.catalyst.optimizer.ConvertToLocalRelation
import org.apache.spark.sql.flint.config.FlintSparkConf.OPTIMIZER_RULE_ENABLED
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

trait FlintPPLSuite extends FlintSparkSuite {
  override protected def sparkConf: SparkConf = {
    val conf = super.sparkConf
      .set(
        "spark.sql.extensions",
        List(
          classOf[IcebergSparkSessionExtensions].getName,
          classOf[FlintPPLSparkExtensions].getName,
          classOf[FlintSparkExtensions].getName)
          .mkString(", "))
      .set(OPTIMIZER_RULE_ENABLED.key, "false")
    conf
  }
}
