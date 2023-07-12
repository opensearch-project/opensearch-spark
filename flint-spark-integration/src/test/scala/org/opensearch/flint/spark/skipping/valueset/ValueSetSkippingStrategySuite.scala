/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.skipping.valueset

import org.scalatest.matchers.should.Matchers

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Abs, And, AttributeReference, EqualTo, GreaterThanOrEqual, Literal}
import org.apache.spark.sql.types.StringType

class ValueSetSkippingStrategySuite extends SparkFunSuite with Matchers {

  private val strategy = ValueSetSkippingStrategy(columnName = "name", columnType = "string")

  private val indexCol = AttributeReference("name", StringType, nullable = false)()

  test("should rewrite EqualTo(<indexCol>, <value>)") {
    strategy.rewritePredicate(EqualTo(indexCol, Literal("hello"))) shouldBe Some(
      EqualTo(UnresolvedAttribute("name"), Literal("hello")))
  }

  test("should not rewrite predicate with other column") {
    val predicate =
      EqualTo(AttributeReference("address", StringType, nullable = false)(), Literal("hello"))

    strategy.rewritePredicate(predicate) shouldBe empty
  }

  test("should not rewrite inapplicable predicate") {
    strategy.rewritePredicate(EqualTo(indexCol, Abs(Literal("hello")))) shouldBe empty
  }

  test("should only rewrite applicable predicate in conjunction") {
    val predicate =
      And(EqualTo(indexCol, Literal("hello")), GreaterThanOrEqual(indexCol, Literal("world")))

    strategy.rewritePredicate(predicate) shouldBe Some(
      EqualTo(UnresolvedAttribute("name"), Literal("hello")))
  }
}
