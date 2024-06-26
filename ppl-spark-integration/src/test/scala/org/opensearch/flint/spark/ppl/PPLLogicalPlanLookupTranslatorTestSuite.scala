/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{Alias, Ascending, Divide, EqualTo, Floor, GreaterThanOrEqual, Literal, Multiply, SortOrder, TimeWindow}
import org.apache.spark.sql.catalyst.plans.logical._
import org.junit.Assert.assertEquals
import org.opensearch.flint.spark.ppl.PlaneUtils.plan
import org.opensearch.sql.ppl.{CatalystPlanContext, CatalystQueryPlanVisitor}
import org.scalatest.matchers.should.Matchers

class PPLLogicalPlanLookupTranslatorTestSuite
    extends SparkFunSuite
    with LogicalPlanTestUtils
    with Matchers {

  private val planTransformer = new CatalystQueryPlanVisitor()
  private val pplParser = new PPLSyntaxParser()

  test("test lookup  ") {
    // if successful build ppl logical plan and translate to catalyst logical plan
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(plan(pplParser, "source = table | lookup a b,c as d appendonly=true q,w as z ", false), context)
    val star = Seq(UnresolvedStar(None))

    val priceField = UnresolvedAttribute("price")
    val tableRelation = UnresolvedRelation(Seq("table"))
    val aggregateExpressions = Seq(
      Alias(UnresolvedFunction(Seq("AVG"), Seq(priceField), isDistinct = false), "avg(price)")())
    val aggregatePlan = Aggregate(Seq(), aggregateExpressions, tableRelation)
    val expectedPlan = Project(star, aggregatePlan)

    //scalastyle:off
    println("### plan ###\n"+compareByString(logPlan)+"\n#########");

    assertEquals(compareByString(expectedPlan), compareByString(logPlan))
  }
}
