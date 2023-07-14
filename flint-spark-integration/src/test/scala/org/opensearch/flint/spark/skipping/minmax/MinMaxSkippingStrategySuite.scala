/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.skipping.minmax

import org.opensearch.flint.spark.skipping.{FlintSparkSkippingStrategy, FlintSparkSkippingStrategySuite}
import org.scalatest.matchers.should.Matchers

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.{Abs, AttributeReference, EqualTo, GreaterThan, GreaterThanOrEqual, In, LessThan, LessThanOrEqual, Literal}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.IntegerType

class MinMaxSkippingStrategySuite
    extends SparkFunSuite
    with FlintSparkSkippingStrategySuite
    with Matchers {

  override val strategy: FlintSparkSkippingStrategy =
    MinMaxSkippingStrategy(columnName = "age", columnType = "integer")

  private val age = AttributeReference("age", IntegerType, nullable = false)()
  private val minAge = col("MinMax_age_0")
  private val maxAge = col("MinMax_age_1")

  test("should rewrite EqualTo(<indexCol>, <value>)") {
    EqualTo(age, Literal(30)) shouldRewriteTo (minAge <= 30 && maxAge >= 30)
  }

  test("should rewrite LessThan(<indexCol>, <value>)") {
    LessThan(age, Literal(30)) shouldRewriteTo (minAge < 30)
  }

  test("should rewrite LessThanOrEqual(<indexCol>, <value>)") {
    LessThanOrEqual(age, Literal(30)) shouldRewriteTo (minAge <= 30)
  }

  test("should rewrite GreaterThan(<indexCol>, <value>)") {
    GreaterThan(age, Literal(30)) shouldRewriteTo (maxAge > 30)
  }

  test("should rewrite GreaterThanOrEqual(<indexCol>, <value>)") {
    GreaterThanOrEqual(age, Literal(30)) shouldRewriteTo (maxAge >= 30)
  }

  test("should rewrite In(<indexCol>, <value1, value2 ...>") {
    val predicate = In(age, Seq(Literal(23), Literal(30), Literal(27)))

    predicate shouldRewriteTo (maxAge >= 23 && minAge <= 30)
  }

  test("should not rewrite inapplicable predicate") {
    EqualTo(age, Abs(Literal(30))) shouldNotRewrite ()
  }
}
