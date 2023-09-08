/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.aggregate.Average
import org.apache.spark.sql.catalyst.expressions.{Alias, NamedExpression}
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

  test("test average price group by product ") {
    // if successful build ppl logical plan and translate to catalyst logical plan
    val context = new CatalystPlanContext
    val logPlan = planTrnasformer.visit(plan(pplParser, "source = table | stats avg(price) by product", false), context)
    //SQL: SELECT product, AVG(price) AS avg_price FROM table GROUP BY product

    val productField = UnresolvedAttribute("product")
    val priceField = UnresolvedAttribute("price")
    val projectList: Seq[NamedExpression] = Seq(UnresolvedStar(None))
    val tableRelation = UnresolvedRelation(Seq("table"))

    val groupByAttributes = Seq(Alias(productField,"product")())
    val aggregateExpressions = Seq(Alias(Average(priceField), "avg(price)")())

    val aggregatePlan = Aggregate(groupByAttributes, aggregateExpressions, tableRelation)
    val expectedPlan = Project(projectList, aggregatePlan)
    
    assertEquals(logPlan, "source=[table] | stats avg(price) by product | fields + *")
    assertEquals(compareByString(expectedPlan), compareByString(context.getPlan))
  }
  
  test("test average price  ") {
    // if successful build ppl logical plan and translate to catalyst logical plan
    val context = new CatalystPlanContext
    val logPlan = planTrnasformer.visit(plan(pplParser, "source = table | stats avg(price) ", false), context)
    //SQL: SELECT avg(price) as avg_price FROM table

    val priceField = UnresolvedAttribute("price")
    val projectList: Seq[NamedExpression] = Seq(UnresolvedStar(None))
    val tableRelation = UnresolvedRelation(Seq("table"))
    val aggregateExpressions = Seq(Alias(Average(priceField), "avg(price)")())

    // Since there's no grouping, we use Nil for the grouping expressions
    val aggregatePlan = Aggregate(Nil, aggregateExpressions, tableRelation)
    val expectedPlan = Project(projectList, aggregatePlan)
    
    assertEquals(logPlan, "source=[table] | stats avg(price) | fields + *")
    assertEquals(compareByString(expectedPlan), compareByString(context.getPlan))
  }
}

