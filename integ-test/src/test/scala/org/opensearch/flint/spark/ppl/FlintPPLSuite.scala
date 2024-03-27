/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import org.opensearch.flint.spark.{FlintPPLSparkExtensions, FlintSparkExtensions, FlintSparkSuite}

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions.CodegenObjectFactoryMode
import org.apache.spark.sql.catalyst.optimizer.ConvertToLocalRelation
import org.apache.spark.sql.flint.config.FlintSparkConf.OPTIMIZER_RULE_ENABLED
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

trait FlintPPLSuite extends FlintSparkSuite {
  override protected def sparkConf: SparkConf = {
    val conf = new SparkConf()
      .set("spark.ui.enabled", "false")
      .set(SQLConf.CODEGEN_FALLBACK.key, "false")
      .set(SQLConf.CODEGEN_FACTORY_MODE.key, CodegenObjectFactoryMode.CODEGEN_ONLY.toString)
      // Disable ConvertToLocalRelation for better test coverage. Test cases built on
      // LocalRelation will exercise the optimization rules better by disabling it as
      // this rule may potentially block testing of other optimization rules such as
      // ConstantPropagation etc.
      .set(SQLConf.OPTIMIZER_EXCLUDED_RULES.key, ConvertToLocalRelation.ruleName)
      .set(
        "spark.sql.extensions",
        List(classOf[FlintPPLSparkExtensions].getName, classOf[FlintSparkExtensions].getName)
          .mkString(", "))
      .set(OPTIMIZER_RULE_ENABLED.key, "false")
    conf
  }
}
