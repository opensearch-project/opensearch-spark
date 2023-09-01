/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.{Abs, AttributeReference, EqualTo, GreaterThan, GreaterThanOrEqual, In, LessThan, LessThanOrEqual, Literal}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.IntegerType
import org.opensearch.flint.spark.skipping.{FlintSparkSkippingStrategy, FlintSparkSkippingStrategySuite}
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.catalyst.analysis.{UnresolvedStar, UnresolvedTable}
import org.junit.Assert.assertEquals

class PPLLogicalPlanTranslatorStrategySuite
  extends SparkFunSuite
  with Matchers {

  private val pplParser = new PPLSyntaxParser()
  private val openSearchAstBuilder = new OpenSearchPPLAstBuilder()

  test("A PPLToCatalystTranslator should correctly translate a simple PPL query") {
    val sqlText = "source=table"
    val tree = pplParser.parse(sqlText)
    val translation = openSearchAstBuilder.visit(tree)

    val projectList: Seq[NamedExpression] = Seq(UnresolvedStar(None))
    val expectedPlan = Project(projectList, UnresolvedTable(Seq("table"), "source=table ", None))

    assertEquals(translation.toString, expectedPlan.toString)
    // Asserts or checks on logicalPlan
    // logicalPlan should ...
  }

  test("it should handle invalid PPL queries gracefully") {
    val sqlText = "select * from table"
    val tree = pplParser.parse(sqlText)
    val translation = openSearchAstBuilder.visit(tree)
    
    // Asserts or checks when invalid PPL queries are passed
    // You can check for exceptions or certain default behavior
    // an [ExceptionType] should be thrownBy { ... }
  }

 

  // Add more test cases as needed
}
