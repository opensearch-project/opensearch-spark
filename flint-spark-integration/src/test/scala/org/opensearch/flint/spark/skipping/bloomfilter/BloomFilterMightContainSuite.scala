/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.skipping.bloomfilter

import org.apache.spark.FlintSuite
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult._
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.types.{BinaryType, DoubleType, LongType, StringType}
import org.apache.spark.unsafe.types.UTF8String

class BloomFilterMightContainSuite extends FlintSuite {

  test("checkInputDataTypes should succeed for valid input types") {
    val binaryExpression = Literal(Array[Byte](1, 2, 3), BinaryType)
    val longExpression = Literal(42L, LongType)

    val bloomFilterMightContain = BloomFilterMightContain(binaryExpression, longExpression)
    assert(bloomFilterMightContain.checkInputDataTypes() == TypeCheckSuccess)
  }

  test("checkInputDataTypes should succeed for valid input types with nulls") {
    val binaryExpression = Literal.create(null, BinaryType)
    val longExpression = Literal.create(null, LongType)

    val bloomFilterMightContain = BloomFilterMightContain(binaryExpression, longExpression)
    assert(bloomFilterMightContain.checkInputDataTypes() == TypeCheckSuccess)
  }

  test("checkInputDataTypes should fail for invalid input types") {
    val stringExpression = Literal(UTF8String.fromString("invalid"), StringType)
    val doubleExpression = Literal(3.14, DoubleType)

    val bloomFilterMightContain = BloomFilterMightContain(stringExpression, doubleExpression)
    val expectedErrorMsg =
      s"""
         | Input to function bloom_filter_might_contain should be Binary expression followed by a Long value,
         | but it's [${stringExpression.dataType.catalogString}, ${doubleExpression.dataType.catalogString}].
         | """.stripMargin

    assert(bloomFilterMightContain.checkInputDataTypes() == TypeCheckFailure(expectedErrorMsg))
  }
}
