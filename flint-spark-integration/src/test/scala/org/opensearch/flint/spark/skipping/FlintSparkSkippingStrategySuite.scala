/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.skipping

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.Expression

trait FlintSparkSkippingStrategySuite {

  /** Subclass initializes strategy class to test */
  val strategy: FlintSparkSkippingStrategy

  /*
   * Add a assertion helpful that provides more readable assertion by
   * infix function: expr shouldRewriteTo col, expr shouldNotRewrite ()
   */
  implicit class EqualityAssertion(left: Expression) {

    def shouldRewriteTo(right: Column): Unit = {
      val actual = strategy.rewritePredicate(left)
      assert(actual.isDefined)
      assert(actual.get == right.expr)
    }

    def shouldNotRewrite(): Unit = {
      val actual = strategy.rewritePredicate(left)
      assert(actual.isEmpty)
    }
  }
}
