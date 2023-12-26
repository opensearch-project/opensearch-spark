/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.function

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar.mock

import org.apache.spark.FlintSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}

class CollectSetLimitSuite extends FlintSuite with Matchers {

  var expression: Expression = _

  override def beforeEach(): Unit = {
    super.beforeEach()

    expression = mock[Expression]
    when(expression.eval(any[InternalRow])).thenAnswer { invocation =>
      val row = invocation.getArgument[InternalRow](0)
      val firstValue = row.getInt(0)
      Literal(firstValue)
    }
  }

  test("should collect unique elements") {
    val collectSetLimit = CollectSetLimit(expression, limit = 2)
    var buffer = collectSetLimit.createAggregationBuffer()

    Seq(InternalRow(1), InternalRow(1)).foreach(row =>
      buffer = collectSetLimit.update(buffer, row))
    assert(buffer.size == 1)
  }

  test("should collect unique elements up to the limit") {
    val collectSetLimit = CollectSetLimit(expression, limit = 2)
    var buffer = collectSetLimit.createAggregationBuffer()

    Seq(InternalRow(1), InternalRow(2), InternalRow(3)).foreach(row =>
      buffer = collectSetLimit.update(buffer, row))
    assert(buffer.size == 2)
  }
}
