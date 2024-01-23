/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.skipping

import org.apache.spark.FlintSuite
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.Expression

trait FlintSparkSkippingStrategySuite extends FlintSuite {

  /** Subclass initializes strategy class to test */
  val strategy: FlintSparkSkippingStrategy

  /** Resolved index expression */
  val indexExpr: Expression

  /*
   * Add a assertion helpful that provides more readable assertion by
   * infix function: expr shouldRewriteTo col, expr shouldNotRewrite ()
   */
  implicit class EqualityAssertion(left: Expression) {

    def shouldRewriteTo(right: Column): Unit = {
      val queryExpr = left
      val actual = strategy.doRewritePredicate(queryExpr, indexExpr)
      assert(actual.isDefined, s"Expected: ${right.expr}. Actual is None")
      assert(actual.get == right.expr, s"Expected: ${right.expr}. Actual: ${actual.get}")
    }

    def shouldNotRewrite(): Unit = {
      val queryExpr = left
      val actual = strategy.doRewritePredicate(queryExpr, indexExpr)
      assert(actual.isEmpty)
    }
  }
}
