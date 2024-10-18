/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import org.opensearch.flint.spark.{FlintPPLSparkExtensions, FlintSparkExtensions, FlintSparkSuite}

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, QueryTest, Row}
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
        List(classOf[FlintPPLSparkExtensions].getName, classOf[FlintSparkExtensions].getName)
          .mkString(", "))
      .set(OPTIMIZER_RULE_ENABLED.key, "false")
    conf
  }

  def assertSameRows(expected: Seq[Row], df: DataFrame): Unit = {
    QueryTest.sameRows(expected, df.collect().toSeq, isSorted = true).foreach { results =>
      fail(s"""
              |Results do not match for query:
              |${df.queryExecution}
              |== Results ==
              |$results
         """.stripMargin)
    }
  }
}
