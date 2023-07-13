/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.skipping.partition

import org.opensearch.flint.spark.skipping.{FlintSparkSkippingStrategy, FlintSparkSkippingStrategySuite}
import org.scalatest.matchers.should.Matchers

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.{Abs, AttributeReference, EqualTo, Literal}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.IntegerType

class PartitionSkippingStrategySuite
    extends SparkFunSuite
    with FlintSparkSkippingStrategySuite
    with Matchers {

  override val strategy: FlintSparkSkippingStrategy =
    PartitionSkippingStrategy(columnName = "year", columnType = "int")

  private val year = AttributeReference("year", IntegerType, nullable = false)()

  test("should rewrite EqualTo(<indexCol>, <value>)") {
    EqualTo(year, Literal(2023)) shouldRewriteTo (col("year") === 2023)
  }

  test("should not rewrite predicate with other column)") {
    val predicate =
      EqualTo(AttributeReference("month", IntegerType, nullable = false)(), Literal(4))

    predicate shouldNotRewrite ()
  }

  test("should not rewrite inapplicable predicate") {
    EqualTo(year, Abs(Literal(2023))) shouldNotRewrite ()
  }
}
