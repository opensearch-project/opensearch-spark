/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.skipping.partition

import org.scalatest.matchers.should.Matchers

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Abs, And, AttributeReference, EqualTo, GreaterThanOrEqual, Literal}
import org.apache.spark.sql.types.IntegerType

class PartitionSkippingStrategySuite extends SparkFunSuite with Matchers {

  private val strategy = PartitionSkippingStrategy(columnName = "year", columnType = "int")

  private val indexCol = AttributeReference("year", IntegerType, nullable = false)()

  test("should rewrite EqualTo(<indexCol>, <value>)") {
    strategy.rewritePredicate(EqualTo(indexCol, Literal(2023))) shouldBe Some(
      EqualTo(UnresolvedAttribute("year"), Literal(2023)))
  }

  test("should not rewrite predicate with other column)") {
    val predicate =
      EqualTo(AttributeReference("month", IntegerType, nullable = false)(), Literal(4))

    strategy.rewritePredicate(predicate) shouldBe empty
  }

  test("should not rewrite inapplicable predicate") {
    strategy.rewritePredicate(EqualTo(indexCol, Abs(Literal(2023)))) shouldBe empty
  }

  test("should only rewrite applicable predicate in conjunction") {
    val predicate =
      And(EqualTo(indexCol, Literal(2023)), GreaterThanOrEqual(indexCol, Literal(2022)))

    strategy.rewritePredicate(predicate) shouldBe Some(
      EqualTo(UnresolvedAttribute("year"), Literal(2023)))
  }
}
