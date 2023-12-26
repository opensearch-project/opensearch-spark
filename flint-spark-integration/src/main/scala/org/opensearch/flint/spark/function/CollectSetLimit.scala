/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.function

import scala.collection.mutable

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription}
import org.apache.spark.sql.catalyst.expressions.aggregate.{Collect, CollectSet, ImperativeAggregate}
import org.apache.spark.sql.types.DataType

/**
 * Collect set of unique values with maximum limit.
 */
@ExpressionDescription(
  usage =
    "_FUNC_(expr, limit) - Collects and returns a set of unique elements up to maximum limit.",
  examples = """
    Examples:
      > SELECT _FUNC_(col, 2) FROM VALUES (1), (2), (1) AS tab(col);
       [1,2]
      > SELECT _FUNC_(col, 1) FROM VALUES (1), (2), (1) AS tab(col);
       [1]
  """,
  note = """
    The function is non-deterministic because the order of collected results depends
    on the order of the rows which may be non-deterministic after a shuffle.
  """,
  group = "agg_funcs",
  since = "2.0.0")
case class CollectSetLimit(
    child: Expression,
    limit: Int,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0)
    extends Collect[mutable.HashSet[Any]] {

  /** Delegate to collect set (because Scala prohibit case-to-case inheritance) */
  private val collectSet = CollectSet(child, mutableAggBufferOffset, inputAggBufferOffset)

  override def update(buffer: mutable.HashSet[Any], input: InternalRow): mutable.HashSet[Any] = {
    if (buffer.size < limit) {
      super.update(buffer, input)
    } else {
      buffer
    }
  }

  override protected def convertToBufferElement(value: Any): Any =
    collectSet.convertToBufferElement(value)

  override protected val bufferElementType: DataType = collectSet.bufferElementType

  override def createAggregationBuffer(): mutable.HashSet[Any] =
    collectSet.createAggregationBuffer()

  override def eval(buffer: mutable.HashSet[Any]): Any = collectSet.eval(buffer)

  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)

  override def withNewMutableAggBufferOffset(
      newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override def prettyName: String = "collect_set_limit"
}

object CollectSetLimit {

  /** Function DSL */
  def collect_set_limit(columnName: String, limit: Int): Column =
    new Column(
      CollectSetLimit(new Column(columnName).expr, limit)
        .toAggregateExpression())
}
