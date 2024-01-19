/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.skipping.valueset

import org.opensearch.flint.spark.skipping.{FlintSparkSkippingStrategy, FlintSparkSkippingStrategySuite}
import org.opensearch.flint.spark.skipping.valueset.ValueSetSkippingStrategy.{DEFAULT_VALUE_SET_MAX_SIZE, VALUE_SET_MAX_SIZE_KEY}
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.{Abs, AttributeReference, EqualTo, Literal}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

class ValueSetSkippingStrategySuite extends SparkFunSuite with FlintSparkSkippingStrategySuite {

  override val strategy: FlintSparkSkippingStrategy =
    ValueSetSkippingStrategy(columnName = "name", columnType = "string")

  private val name = AttributeReference("name", StringType, nullable = false)()

  test("should return parameters with default value") {
    strategy.parameters shouldBe Map(
      VALUE_SET_MAX_SIZE_KEY -> DEFAULT_VALUE_SET_MAX_SIZE.toString)
  }

  test("should build aggregator with default parameter") {
    strategy.getAggregators.head.semanticEquals(
      when(size(collect_set("name")) > DEFAULT_VALUE_SET_MAX_SIZE, lit(null))
        .otherwise(collect_set("name"))
        .expr) shouldBe true
  }

  test("should use given parameter value") {
    val strategy =
      ValueSetSkippingStrategy(
        columnName = "name",
        columnType = "string",
        params = Map(VALUE_SET_MAX_SIZE_KEY -> "5"))

    strategy.parameters shouldBe Map(VALUE_SET_MAX_SIZE_KEY -> "5")
    strategy.getAggregators.head.semanticEquals(
      when(size(collect_set("name")) > 5, lit(null))
        .otherwise(collect_set("name"))
        .expr) shouldBe true
  }

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
