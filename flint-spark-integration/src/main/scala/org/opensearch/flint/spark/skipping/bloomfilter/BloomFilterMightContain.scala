/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.skipping.bloomfilter

import java.io.ByteArrayInputStream

import org.opensearch.flint.core.field.bloomfilter.classic.ClassicBloomFilter

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.{BinaryComparison, Expression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types._

/**
 * Bloom filter function that returns the membership check result for values of `valueExpression`
 * in the bloom filter represented by `bloomFilterExpression`.
 *
 * @param bloomFilterExpression
 *   binary expression that represents bloom filter data
 * @param valueExpression
 *   Long value expression to be tested
 */
case class BloomFilterMightContain(bloomFilterExpression: Expression, valueExpression: Expression)
    extends BinaryComparison {

  override def nullable: Boolean = true

  override def left: Expression = bloomFilterExpression

  override def right: Expression = valueExpression

  override def prettyName: String = "bloom_filter_might_contain"

  override def dataType: DataType = BooleanType

  override def symbol: String = "BLOOM_FILTER_MIGHT_CONTAIN"

  override def checkInputDataTypes(): TypeCheckResult = {
    (left.dataType, right.dataType) match {
      case (BinaryType, NullType) | (NullType, LongType) | (NullType, NullType) |
          (BinaryType, LongType) =>
        TypeCheckResult.TypeCheckSuccess
      case _ =>
        TypeCheckResult.TypeCheckFailure(s"""
           | Input to function $prettyName should be Binary expression followed by a Long value,
           | but it's [${left.dataType.catalogString}, ${right.dataType.catalogString}].
           | """.stripMargin)
    }
  }

  override protected def withNewChildrenInternal(
      newBloomFilterExpression: Expression,
      newValueExpression: Expression): BloomFilterMightContain =
    copy(bloomFilterExpression = newBloomFilterExpression, valueExpression = newValueExpression)

  override def eval(input: InternalRow): Any = {
    val value = valueExpression.eval(input)
    if (value == null) {
      null
    } else {
      val bytes = bloomFilterExpression.eval(input).asInstanceOf[Array[Byte]]
      val bloomFilter = ClassicBloomFilter.readFrom(new ByteArrayInputStream(bytes))
      bloomFilter.mightContain(value.asInstanceOf[Long])
    }
  }

  /**
   * Generate expression code for Spark codegen execution. Sample result code:
   * ```
   *   boolean filter_isNull_0 = true;
   *   boolean filter_value_0 = false;
   *   if (!right_isNull) {
   *     filter_isNull_0 = false;
   *     filter_value_0 =
   *       org.opensearch.flint.core.field.bloomfilter.classic.ClassicBloomFilter.readFrom(
   *         new java.io.ByteArrayInputStream(left_value)
   *       ).mightContain(right_value);
   *   }
   * ```
   */
  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val leftGen = left.genCode(ctx)
    val rightGen = right.genCode(ctx)
    val bloomFilterEncoder = classOf[ClassicBloomFilter].getCanonicalName.stripSuffix("$")
    val bf = s"$bloomFilterEncoder.readFrom(new java.io.ByteArrayInputStream(${leftGen.value}))"
    val result = s"$bf.mightContain(${rightGen.value})"
    val resultCode =
      s"""
         |if (!(${rightGen.isNull})) {
         |  ${leftGen.code}
         |  ${ev.isNull} = false;
         |  ${ev.value} = $result;
         |}
       """.stripMargin
    ev.copy(code = code"""
      ${rightGen.code}
      boolean ${ev.isNull} = true;
      boolean ${ev.value} = false;
      $resultCode""")
  }
}

object BloomFilterMightContain {

  /**
   * Generate bloom filter might contain function given the bloom filter column and value.
   *
   * @param colName
   *   column name
   * @param value
   *   value
   * @return
   *   bloom filter might contain expression
   */
  def bloom_filter_might_contain(colName: String, value: Any): Column = {
    new Column(BloomFilterMightContain(col(colName).expr, lit(value).expr))
  }
}
