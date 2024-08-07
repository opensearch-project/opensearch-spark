/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import org.opensearch.flint.spark.ppl.PlaneUtils.plan
import org.opensearch.sql.ppl.{CatalystPlanContext, CatalystQueryPlanVisitor}
import org.scalatest.matchers.should.Matchers

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{Alias, Ascending, Divide, EqualTo, Floor, GreaterThanOrEqual, Literal, Multiply, SortOrder, TimeWindow}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._

class PPLLogicalPlanAggregationQueriesTranslatorTestSuite
    extends SparkFunSuite
    with PlanTest
    with LogicalPlanTestUtils
    with Matchers {

  private val planTransformer = new CatalystQueryPlanVisitor()
  private val pplParser = new PPLSyntaxParser()

  test("test average price") {
    // if successful build ppl logical plan and translate to catalyst logical plan
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(plan(pplParser, "source = table | stats avg(price) ", false), context)
    // SQL: SELECT avg(price) as avg_price FROM table
    val star = Seq(UnresolvedStar(None))

    val priceField = UnresolvedAttribute("price")
    val tableRelation = UnresolvedRelation(Seq("table"))
    val aggregateExpressions = Seq(
      Alias(UnresolvedFunction(Seq("AVG"), Seq(priceField), isDistinct = false), "avg(price)")())
    val aggregatePlan = Aggregate(Seq(), aggregateExpressions, tableRelation)
    val expectedPlan = Project(star, aggregatePlan)

    comparePlans(expectedPlan, logPlan, false)
  }

  test("test average price with Alias") {
    // if successful build ppl logical plan and translate to catalyst logical plan
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(
      plan(pplParser, "source = table | stats avg(price) as avg_price", false),
      context)
    // SQL: SELECT avg(price) as avg_price FROM table
    val star = Seq(UnresolvedStar(None))

    val priceField = UnresolvedAttribute("price")
    val tableRelation = UnresolvedRelation(Seq("table"))
    val aggregateExpressions = Seq(
      Alias(UnresolvedFunction(Seq("AVG"), Seq(priceField), isDistinct = false), "avg_price")())
    val aggregatePlan = Aggregate(Seq(), aggregateExpressions, tableRelation)
    val expectedPlan = Project(star, aggregatePlan)

    comparePlans(expectedPlan, logPlan, false)
  }

  test("test average price group by product ") {
    // if successful build ppl logical plan and translate to catalyst logical plan
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(
      plan(pplParser, "source = table | stats avg(price) by product", false),
      context)
    // SQL: SELECT product, AVG(price) AS avg_price FROM table GROUP BY product
    val star = Seq(UnresolvedStar(None))
    val productField = UnresolvedAttribute("product")
    val priceField = UnresolvedAttribute("price")
    val tableRelation = UnresolvedRelation(Seq("table"))

    val groupByAttributes = Seq(Alias(productField, "product")())
    val aggregateExpressions =
      Alias(UnresolvedFunction(Seq("AVG"), Seq(priceField), isDistinct = false), "avg(price)")()
    val productAlias = Alias(productField, "product")()

    val aggregatePlan =
      Aggregate(groupByAttributes, Seq(aggregateExpressions, productAlias), tableRelation)
    val expectedPlan = Project(star, aggregatePlan)

    comparePlans(expectedPlan, logPlan, false)
  }

  test("test average price group by product and filter") {
    // if successful build ppl logical plan and translate to catalyst logical plan
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(
      plan(pplParser, "source = table country ='USA' | stats avg(price) by product", false),
      context)
    // SQL: SELECT product, AVG(price) AS avg_price FROM table GROUP BY product
    val star = Seq(UnresolvedStar(None))
    val productField = UnresolvedAttribute("product")
    val priceField = UnresolvedAttribute("price")
    val countryField = UnresolvedAttribute("country")
    val table = UnresolvedRelation(Seq("table"))

    val groupByAttributes = Seq(Alias(productField, "product")())
    val aggregateExpressions =
      Alias(UnresolvedFunction(Seq("AVG"), Seq(priceField), isDistinct = false), "avg(price)")()
    val productAlias = Alias(productField, "product")()

    val filterExpr = EqualTo(countryField, Literal("USA"))
    val filterPlan = Filter(filterExpr, table)

    val aggregatePlan =
      Aggregate(groupByAttributes, Seq(aggregateExpressions, productAlias), filterPlan)
    val expectedPlan = Project(star, aggregatePlan)

    comparePlans(expectedPlan, logPlan, false)
  }

  test("test average price group by product and filter sorted") {
    // if successful build ppl logical plan and translate to catalyst logical plan
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(
      plan(
        pplParser,
        "source = table country ='USA' | stats avg(price) by product | sort product",
        false),
      context)
    // SQL: SELECT product, AVG(price) AS avg_price FROM table GROUP BY product
    val star = Seq(UnresolvedStar(None))
    val productField = UnresolvedAttribute("product")
    val priceField = UnresolvedAttribute("price")
    val countryField = UnresolvedAttribute("country")
    val table = UnresolvedRelation(Seq("table"))

    val groupByAttributes = Seq(Alias(productField, "product")())
    val aggregateExpressions =
      Alias(UnresolvedFunction(Seq("AVG"), Seq(priceField), isDistinct = false), "avg(price)")()
    val productAlias = Alias(productField, "product")()

    val filterExpr = EqualTo(countryField, Literal("USA"))
    val filterPlan = Filter(filterExpr, table)

    val aggregatePlan =
      Aggregate(groupByAttributes, Seq(aggregateExpressions, productAlias), filterPlan)
    val sortedPlan: LogicalPlan =
      Sort(
        Seq(SortOrder(UnresolvedAttribute("product"), Ascending)),
        global = true,
        aggregatePlan)
    val expectedPlan = Project(star, sortedPlan)
    comparePlans(expectedPlan, logPlan, false)
  }
  test("create ppl simple avg age by span of interval of 10 years query test ") {
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(
      plan(pplParser, "source = table | stats avg(age) by span(age, 10) as age_span", false),
      context)
    // Define the expected logical plan
    val star = Seq(UnresolvedStar(None))
    val ageField = UnresolvedAttribute("age")
    val tableRelation = UnresolvedRelation(Seq("table"))

    val aggregateExpressions =
      Alias(UnresolvedFunction(Seq("AVG"), Seq(ageField), isDistinct = false), "avg(age)")()
    val span = Alias(
      Multiply(Floor(Divide(UnresolvedAttribute("age"), Literal(10))), Literal(10)),
      "age_span")()
    val aggregatePlan = Aggregate(Seq(span), Seq(aggregateExpressions, span), tableRelation)
    val expectedPlan = Project(star, aggregatePlan)

    comparePlans(expectedPlan, logPlan, false)
  }

  test("create ppl simple avg age by span of interval of 10 years query with sort test ") {
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(
      plan(
        pplParser,
        "source = table | stats avg(age) by span(age, 10) as age_span | sort age",
        false),
      context)
    // Define the expected logical plan
    val star = Seq(UnresolvedStar(None))
    val ageField = UnresolvedAttribute("age")
    val tableRelation = UnresolvedRelation(Seq("table"))

    val aggregateExpressions =
      Alias(UnresolvedFunction(Seq("AVG"), Seq(ageField), isDistinct = false), "avg(age)")()
    val span = Alias(
      Multiply(Floor(Divide(UnresolvedAttribute("age"), Literal(10))), Literal(10)),
      "age_span")()
    val aggregatePlan = Aggregate(Seq(span), Seq(aggregateExpressions, span), tableRelation)
    val sortedPlan: LogicalPlan =
      Sort(Seq(SortOrder(UnresolvedAttribute("age"), Ascending)), global = true, aggregatePlan)
    val expectedPlan = Project(star, sortedPlan)

    comparePlans(expectedPlan, logPlan, false)
  }

  test("create ppl simple avg age by span of interval of 10 years by country query test ") {
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(
      plan(
        pplParser,
        "source = table | stats avg(age) by span(age, 10) as age_span, country",
        false),
      context)
    // Define the expected logical plan
    val star = Seq(UnresolvedStar(None))
    val ageField = UnresolvedAttribute("age")
    val tableRelation = UnresolvedRelation(Seq("table"))
    val countryField = UnresolvedAttribute("country")
    val countryAlias = Alias(countryField, "country")()

    val aggregateExpressions =
      Alias(UnresolvedFunction(Seq("AVG"), Seq(ageField), isDistinct = false), "avg(age)")()
    val span = Alias(
      Multiply(Floor(Divide(UnresolvedAttribute("age"), Literal(10))), Literal(10)),
      "age_span")()
    val aggregatePlan = Aggregate(
      Seq(countryAlias, span),
      Seq(aggregateExpressions, countryAlias, span),
      tableRelation)
    val expectedPlan = Project(star, aggregatePlan)

    comparePlans(expectedPlan, logPlan, false)
  }
  test("create ppl query count sales by weeks window and productId with sorting test") {
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(
      plan(
        pplParser,
        "source = table | stats sum(productsAmount) by span(transactionDate, 1w) as age_date | sort age_date",
        false),
      context)

    // Define the expected logical plan
    val star = Seq(UnresolvedStar(None))
    val productsAmount = UnresolvedAttribute("productsAmount")
    val table = UnresolvedRelation(Seq("table"))

    val windowExpression = Alias(
      TimeWindow(
        UnresolvedAttribute("transactionDate"),
        TimeWindow.parseExpression(Literal("1 week")),
        TimeWindow.parseExpression(Literal("1 week")),
        0),
      "age_date")()

    val aggregateExpressions =
      Alias(
        UnresolvedFunction(Seq("SUM"), Seq(productsAmount), isDistinct = false),
        "sum(productsAmount)")()
    val aggregatePlan =
      Aggregate(Seq(windowExpression), Seq(aggregateExpressions, windowExpression), table)

    val sortedPlan: LogicalPlan = Sort(
      Seq(SortOrder(UnresolvedAttribute("age_date"), Ascending)),
      global = true,
      aggregatePlan)

    val expectedPlan = Project(star, sortedPlan)
    // Compare the two plans
    comparePlans(expectedPlan, logPlan, false)
  }

  test("create ppl query count sales by days window and productId with sorting test") {
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(
      plan(
        pplParser,
        "source = table | stats sum(productsAmount) by span(transactionDate, 1d) as age_date, productId | sort age_date",
        false),
      context)
    // Define the expected logical plan
    val star = Seq(UnresolvedStar(None))
    val productsId = Alias(UnresolvedAttribute("productId"), "productId")()
    val productsAmount = UnresolvedAttribute("productsAmount")
    val table = UnresolvedRelation(Seq("table"))

    val windowExpression = Alias(
      TimeWindow(
        UnresolvedAttribute("transactionDate"),
        TimeWindow.parseExpression(Literal("1 day")),
        TimeWindow.parseExpression(Literal("1 day")),
        0),
      "age_date")()

    val aggregateExpressions =
      Alias(
        UnresolvedFunction(Seq("SUM"), Seq(productsAmount), isDistinct = false),
        "sum(productsAmount)")()
    val aggregatePlan = Aggregate(
      Seq(productsId, windowExpression),
      Seq(aggregateExpressions, productsId, windowExpression),
      table)
    val sortedPlan: LogicalPlan = Sort(
      Seq(SortOrder(UnresolvedAttribute("age_date"), Ascending)),
      global = true,
      aggregatePlan)
    val expectedPlan = Project(star, sortedPlan)
    // Compare the two plans
    comparePlans(expectedPlan, logPlan, false)
  }
  test("create ppl query count status amount by day window and group by status test") {
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(
      plan(
        pplParser,
        "source = table | stats sum(status) by span(@timestamp, 1d) as status_count_by_day, status | head 100",
        false),
      context)
    // Define the expected logical plan
    val star = Seq(UnresolvedStar(None))
    val status = Alias(UnresolvedAttribute("status"), "status")()
    val statusAmount = UnresolvedAttribute("status")
    val table = UnresolvedRelation(Seq("table"))

    val windowExpression = Alias(
      TimeWindow(
        UnresolvedAttribute("`@timestamp`"),
        TimeWindow.parseExpression(Literal("1 day")),
        TimeWindow.parseExpression(Literal("1 day")),
        0),
      "status_count_by_day")()

    val aggregateExpressions =
      Alias(
        UnresolvedFunction(Seq("SUM"), Seq(statusAmount), isDistinct = false),
        "sum(status)")()
    val aggregatePlan = Aggregate(
      Seq(status, windowExpression),
      Seq(aggregateExpressions, status, windowExpression),
      table)
    val planWithLimit = GlobalLimit(Literal(100), LocalLimit(Literal(100), aggregatePlan))
    val expectedPlan = Project(star, planWithLimit)
    // Compare the two plans
    comparePlans(expectedPlan, logPlan, false)
  }
  test(
    "create ppl query count only error (status >= 400) status amount by day window and group by status test") {
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(
      plan(
        pplParser,
        "source = table | where status >= 400 | stats sum(status) by span(@timestamp, 1d) as status_count_by_day, status | head 100",
        false),
      context)
    // Define the expected logical plan
    val star = Seq(UnresolvedStar(None))
    val statusAlias = Alias(UnresolvedAttribute("status"), "status")()
    val statusField = UnresolvedAttribute("status")
    val table = UnresolvedRelation(Seq("table"))

    val filterExpr = GreaterThanOrEqual(statusField, Literal(400))
    val filterPlan = Filter(filterExpr, table)

    val windowExpression = Alias(
      TimeWindow(
        UnresolvedAttribute("`@timestamp`"),
        TimeWindow.parseExpression(Literal("1 day")),
        TimeWindow.parseExpression(Literal("1 day")),
        0),
      "status_count_by_day")()

    val aggregateExpressions =
      Alias(UnresolvedFunction(Seq("SUM"), Seq(statusField), isDistinct = false), "sum(status)")()
    val aggregatePlan = Aggregate(
      Seq(statusAlias, windowExpression),
      Seq(aggregateExpressions, statusAlias, windowExpression),
      filterPlan)
    val planWithLimit = GlobalLimit(Literal(100), LocalLimit(Literal(100), aggregatePlan))
    val expectedPlan = Project(star, planWithLimit)
    // Compare the two plans
    comparePlans(expectedPlan, logPlan, false)
  }

}
