/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction, UnresolvedRelation}
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.plans.logical._
import org.junit.Assert.assertEquals
import org.opensearch.flint.spark.ppl.PlaneUtils.plan
import org.opensearch.sql.ppl.{CatalystPlanContext, CatalystQueryPlanVisitor}
import org.scalatest.matchers.should.Matchers

class PPLLogicalPlanAggregationQueriesTranslatorTestSuite
  extends SparkFunSuite
    with LogicalPlanTestUtils
    with Matchers {

  private val planTrnasformer = new CatalystQueryPlanVisitor()
  private val pplParser = new PPLSyntaxParser()
  
  test("test average price  ") {
    // if successful build ppl logical plan and translate to catalyst logical plan
    val context = new CatalystPlanContext
    val logPlan = planTrnasformer.visit(plan(pplParser, "source = table | stats avg(price) ", false), context)
    //SQL: SELECT avg(price) as avg_price FROM table

    val priceField = UnresolvedAttribute("price")
    val tableRelation = UnresolvedRelation(Seq("table"))
    val aggregateExpressions = Seq(Alias(UnresolvedFunction(Seq("AVG"), Seq(priceField), isDistinct = false), "avg(price)")())
    val aggregatePlan = Project( aggregateExpressions, tableRelation)
    
    assertEquals(logPlan, "source=[table] | stats avg(price) | fields + 'AVG('price) AS avg(price)#0")
    assertEquals(compareByString(aggregatePlan), compareByString(context.getPlan))
  }

  ignore("test average price group by product ") {
    // if successful build ppl logical plan and translate to catalyst logical plan
    val context = new CatalystPlanContext
    val logPlan = planTrnasformer.visit(plan(pplParser, "source = table | stats avg(price) by product", false), context)
    //SQL: SELECT product, AVG(price) AS avg_price FROM table GROUP BY product

    val productField = UnresolvedAttribute("product")
    val priceField = UnresolvedAttribute("price")
    val tableRelation = UnresolvedRelation(Seq("table"))

    val groupByAttributes = Seq(Alias(productField, "product")())
    val aggregateExpressions = Seq(Alias(UnresolvedFunction(Seq("AVG"), Seq(priceField), isDistinct = false), "avg(price)")())

    val aggregatePlan = Aggregate(groupByAttributes, aggregateExpressions, tableRelation)
    val expectedPlan = Project(Seq(productField), aggregatePlan)

    assertEquals(logPlan, "source=[table] | stats avg(price) by product | fields + 'product AS product#1")
    assertEquals(compareByString(expectedPlan), compareByString(context.getPlan))
  }

}

