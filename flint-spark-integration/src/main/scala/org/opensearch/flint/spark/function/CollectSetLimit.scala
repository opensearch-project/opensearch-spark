/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.function

import scala.collection.mutable

import org.opensearch.flint.spark.function.CollectSetLimit.FUNCTION_NAME

import org.apache.spark.sql.catalyst.{FunctionIdentifier, InternalRow}
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.analysis.FunctionRegistryBase
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription, ExpressionInfo}
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
      > SELECT _FUNC_(col) FROM VALUES (1), (2), (1) AS tab(col);
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

  /** Avoid re-creating empty set after limit reached */
  private val emptySetAfterLimitReached = mutable.HashSet.empty[Any]

  /** Is limit reached already (buffer will be empty so cannot rely on its size) */
  private var limitReached = false

  /** Delegate to collect set (because Scala prohibit case-to-case inheritance) */
  private val collectSet = CollectSet(child, mutableAggBufferOffset, inputAggBufferOffset)

  override def update(buffer: mutable.HashSet[Any], input: InternalRow): mutable.HashSet[Any] = {
    if (limitReached) {
      // Keep returning empty set with no-op after limit reached
      emptySetAfterLimitReached
    } else {
      // Update first and then check limit because only unique element added
      val newBuffer = super.update(buffer, input)
      if (newBuffer.size <= limit) {
        newBuffer
      } else {
        // Mark limit reached
        limitReached = true
        emptySetAfterLimitReached
      }
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

  override def prettyName: String = FUNCTION_NAME
}

object CollectSetLimit {

  val FUNCTION_NAME = "collect_set_limit"

  def description: (FunctionIdentifier, ExpressionInfo, FunctionBuilder) = {
    val (expressionInfo, funcBuilder) =
      FunctionRegistryBase.build[CollectSetLimit](FUNCTION_NAME, None)
    (FunctionIdentifier(FUNCTION_NAME), expressionInfo, funcBuilder)
  }
}
