/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.skipping.valueset

import org.opensearch.flint.spark.skipping.{FlintSparkSkippingStrategy, FlintSparkSkippingStrategySuite}
import org.scalatest.matchers.should.Matchers

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.{Abs, And, AttributeReference, EqualTo, GreaterThanOrEqual, Literal}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StringType

class ValueSetSkippingStrategySuite
    extends SparkFunSuite
    with FlintSparkSkippingStrategySuite
    with Matchers {

  override val strategy: FlintSparkSkippingStrategy =
    ValueSetSkippingStrategy(columnName = "name", columnType = "string")

  private val name = AttributeReference("name", StringType, nullable = false)()

  test("should rewrite EqualTo(<indexCol>, <value>)") {
    EqualTo(name, Literal("hello")) shouldRewriteTo (col("name") === "hello")
  }

  test("should not rewrite predicate with other column") {
    val predicate =
      EqualTo(AttributeReference("address", StringType, nullable = false)(), Literal("hello"))

    predicate shouldNotRewrite ()
  }

  test("should not rewrite inapplicable predicate") {
    EqualTo(name, Abs(Literal("hello"))) shouldNotRewrite ()
  }

  test("should only rewrite applicable predicate in conjunction") {
    val predicate =
      And(EqualTo(name, Literal("hello")), GreaterThanOrEqual(name, Literal("world")))

    predicate shouldRewriteTo (col("name") === "hello")
  }
}
