/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import org.junit.Assert.assertEquals
import org.opensearch.flint.spark.ppl.PlaneUtils.plan
import org.opensearch.sql.ppl.{CatalystPlanContext, CatalystQueryPlanVisitor}
import org.scalatest.matchers.should.Matchers

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{Alias, Ascending, Divide, EqualTo, Floor, Literal, Multiply, SortOrder, TimeWindow}
import org.apache.spark.sql.catalyst.plans.logical._

class PPLLogicalPlanAggregationQueriesTranslatorTestSuite
    extends SparkFunSuite
    with LogicalPlanTestUtils
    with Matchers {

  private val planTrnasformer = new CatalystQueryPlanVisitor()
  private val pplParser = new PPLSyntaxParser()

  test("test average price  ") {
    // if successful build ppl logical plan and translate to catalyst logical plan
    val context = new CatalystPlanContext
    val logPlan =
      planTrnasformer.visit(plan(pplParser, "source = table | stats avg(price) ", false), context)
    // SQL: SELECT avg(price) as avg_price FROM table
    val star = Seq(UnresolvedStar(None))

    val priceField = UnresolvedAttribute("price")
    val tableRelation = UnresolvedRelation(Seq("table"))
    val aggregateExpressions = Seq(
      Alias(UnresolvedFunction(Seq("AVG"), Seq(priceField), isDistinct = false), "avg(price)")())
    val aggregatePlan = Aggregate(Seq(), aggregateExpressions, tableRelation)
    val expectedPlan = Project(star, aggregatePlan)

    assertEquals(compareByString(expectedPlan), compareByString(logPlan))
  }

  ignore("test average price with Alias") {
    // if successful build ppl logical plan and translate to catalyst logical plan
    val context = new CatalystPlanContext
    val logPlan = planTrnasformer.visit(
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

    assertEquals(compareByString(expectedPlan), compareByString(logPlan))
  }

  test("test average price group by product ") {
    // if successful build ppl logical plan and translate to catalyst logical plan
    val context = new CatalystPlanContext
    val logPlan = planTrnasformer.visit(
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

    assertEquals(compareByString(expectedPlan), compareByString(logPlan))
  }

  test("test average price group by product and filter") {
    // if successful build ppl logical plan and translate to catalyst logical plan
    val context = new CatalystPlanContext
    val logPlan = planTrnasformer.visit(
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

    assertEquals(compareByString(expectedPlan), compareByString(logPlan))
  }

  test("test average price group by product and filter sorted") {
    // if successful build ppl logical plan and translate to catalyst logical plan
    val context = new CatalystPlanContext
    val logPlan = planTrnasformer.visit(
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
    assertEquals(compareByString(expectedPlan), compareByString(logPlan))
  }
  test("create ppl simple avg age by span of interval of 10 years query test ") {
    val context = new CatalystPlanContext
    val logPlan = planTrnasformer.visit(
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

    assert(compareByString(expectedPlan) === compareByString(logPlan))
  }

  test("create ppl simple avg age by span of interval of 10 years query with sort test ") {
    val context = new CatalystPlanContext
    val logPlan = planTrnasformer.visit(
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

    assert(compareByString(expectedPlan) === compareByString(logPlan))
  }

  test("create ppl simple avg age by span of interval of 10 years by country query test ") {
    val context = new CatalystPlanContext
    val logPlan = planTrnasformer.visit(
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

    assert(compareByString(expectedPlan) === compareByString(logPlan))
  }
  test("create ppl query count sales by weeks window and productId with sorting test") {
    val context = new CatalystPlanContext
    val logPlan = planTrnasformer.visit(
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
    assert(compareByString(expectedPlan) === compareByString(logPlan))
  }

  test("create ppl query count sales by days window and productId with sorting test") {
    val context = new CatalystPlanContext
    val logPlan = planTrnasformer.visit(
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
    assert(compareByString(expectedPlan) === compareByString(logPlan))
  }

}
