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
import org.opensearch.sql.ppl.utils.StatementUtils
import org.opensearch.sql.ppl.{CatalystPlanContext, CatalystQueryPlanVisitor}

class PPLLogicalPlanTranslatorStrategySuite
  extends SparkFunSuite
  with Matchers {

  private val pplParser = new PPLSyntaxParser()
  private val planTrnasormer = new CatalystQueryPlanVisitor()

  test("A PPLToCatalystTranslator should correctly translate a simple PPL query") {
    val sqlText = "source=table"
    val context = new CatalystPlanContext
    planTrnasormer.visit(StatementUtils.plan(pplParser, sqlText, false), context)
    val plan = context.getPlan

    val projectList: Seq[NamedExpression] = Seq(UnresolvedStar(None))
    val expectedPlan = Project(projectList, UnresolvedTable(Seq("table"), "source=table ", None))

    assertEquals(plan.toString, expectedPlan.toString)
    // Asserts or checks on logicalPlan
    // logicalPlan should ...
  }

  test("it should handle invalid PPL queries gracefully") {
    val sqlText = "select * from table"
    
    // Asserts or checks when invalid PPL queries are passed
    // You can check for exceptions or certain default behavior
    // an [ExceptionType] should be thrownBy { ... }
  }

 

  // Add more test cases as needed
}
