/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.skipping.valueset

import org.opensearch.flint.spark.skipping.{FlintSparkSkippingStrategy, FlintSparkSkippingStrategySuite}
import org.scalatest.matchers.should.Matchers

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.{Abs, AttributeReference, EqualTo, Literal}
import org.apache.spark.sql.functions.{col, isnull}
import org.apache.spark.sql.types.StringType

class ValueSetSkippingStrategySuite
    extends SparkFunSuite
    with FlintSparkSkippingStrategySuite
    with Matchers {

  override val strategy: FlintSparkSkippingStrategy =
    ValueSetSkippingStrategy(columnName = "name", columnType = "string")

  private val name = AttributeReference("name", StringType, nullable = false)()

  test("should rewrite EqualTo(<indexCol>, <value>)") {
    EqualTo(name, Literal("hello")) shouldRewriteTo
      (isnull(col("name")) || col("name") === "hello")
  }

  test("should not rewrite predicate with other column") {
    val predicate =
      EqualTo(AttributeReference("address", StringType, nullable = false)(), Literal("hello"))

    predicate shouldNotRewrite ()
  }

  test("should not rewrite inapplicable predicate") {
    EqualTo(name, Abs(Literal("hello"))) shouldNotRewrite ()
  }
}
