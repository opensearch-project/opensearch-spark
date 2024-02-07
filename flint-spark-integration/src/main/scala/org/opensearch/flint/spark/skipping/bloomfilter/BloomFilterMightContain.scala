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
import org.apache.spark.sql.types.{BooleanType, DataType}

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

  override def checkInputDataTypes(): TypeCheckResult = TypeCheckResult.TypeCheckSuccess

  override protected def withNewChildrenInternal(
      newBloomFilterExpression: Expression,
      newValueExpression: Expression): BloomFilterMightContain =
    copy(bloomFilterExpression = newBloomFilterExpression, valueExpression = newValueExpression)

  override def eval(input: InternalRow): Any = {
    val value = valueExpression.eval(input)
    if (value == null) {
      null
    } else {
      val bytes = bloomFilterExpression.eval().asInstanceOf[Array[Byte]]
      val bloomFilter = ClassicBloomFilter.readFrom(new ByteArrayInputStream(bytes))
      bloomFilter.mightContain(value.asInstanceOf[Long])
    }
  }

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
