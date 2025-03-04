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
import org.apache.spark.sql.catalyst.expressions.{Alias, Ascending, Divide, EqualTo, Floor, GreaterThan, GreaterThanOrEqual, Literal, Multiply, SortOrder, TimeWindow}
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
      planTransformer.visit(plan(pplParser, "source = table | stats avg(price) "), context)
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
      plan(pplParser, "source = table | stats avg(price) as avg_price"),
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
      plan(pplParser, "source = table | stats avg(price) by product"),
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
      plan(pplParser, "source = table country ='USA' | stats avg(price) by product"),
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
        "source = table country ='USA' | stats avg(price) by product | sort product"),
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
      plan(pplParser, "source = table | stats avg(age) by span(age, 10) as age_span"),
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
      plan(pplParser, "source = table | stats avg(age) by span(age, 10) as age_span | sort age"),
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
      plan(pplParser, "source = table | stats avg(age) by span(age, 10) as age_span, country"),
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
        "source = table | stats sum(productsAmount) by span(transactionDate, 1w) as age_date | sort age_date"),
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
        "source = table | stats sum(productsAmount) by span(transactionDate, 1d) as age_date, productId | sort age_date"),
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
        "source = table | stats sum(status) by span(@timestamp, 1d) as status_count_by_day, status | head 100"),
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
        "source = table | where status >= 400 | stats sum(status) by span(@timestamp, 1d) as status_count_by_day, status | head 100"),
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

  test("test price sample stddev group by product sorted") {
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(
      plan(pplParser, "source = table | stats stddev_samp(price) by product | sort product"),
      context)
    val star = Seq(UnresolvedStar(None))
    val priceField = UnresolvedAttribute("price")
    val productField = UnresolvedAttribute("product")
    val tableRelation = UnresolvedRelation(Seq("table"))

    val groupByAttributes = Seq(Alias(productField, "product")())
    val aggregateExpressions =
      Alias(
        UnresolvedFunction(Seq("STDDEV_SAMP"), Seq(priceField), isDistinct = false),
        "stddev_samp(price)")()
    val productAlias = Alias(productField, "product")()

    val aggregatePlan =
      Aggregate(groupByAttributes, Seq(aggregateExpressions, productAlias), tableRelation)
    val sortedPlan: LogicalPlan =
      Sort(Seq(SortOrder(productField, Ascending)), global = true, aggregatePlan)
    val expectedPlan = Project(star, sortedPlan)

    comparePlans(expectedPlan, logPlan, false)
  }

  test("test price sample stddev with alias and filter") {
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(
      plan(
        pplParser,
        "source = table category = 'vegetable' | stats stddev_samp(price) as dev_samp"),
      context)
    val star = Seq(UnresolvedStar(None))
    val categoryField = UnresolvedAttribute("category")
    val priceField = UnresolvedAttribute("price")
    val tableRelation = UnresolvedRelation(Seq("table"))

    val aggregateExpressions = Seq(
      Alias(
        UnresolvedFunction(Seq("STDDEV_SAMP"), Seq(priceField), isDistinct = false),
        "dev_samp")())
    val filterExpr = EqualTo(categoryField, Literal("vegetable"))
    val filterPlan = Filter(filterExpr, tableRelation)
    val aggregatePlan = Aggregate(Seq(), aggregateExpressions, filterPlan)
    val expectedPlan = Project(star, aggregatePlan)

    comparePlans(expectedPlan, logPlan, false)
  }

  test("test age sample stddev by span of interval of 5 years query with sort ") {
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(
      plan(
        pplParser,
        "source = table | stats stddev_samp(age) by span(age, 5) as age_span | sort age"),
      context)
    // Define the expected logical plan
    val star = Seq(UnresolvedStar(None))
    val ageField = UnresolvedAttribute("age")
    val tableRelation = UnresolvedRelation(Seq("table"))

    val aggregateExpressions =
      Alias(
        UnresolvedFunction(Seq("STDDEV_SAMP"), Seq(ageField), isDistinct = false),
        "stddev_samp(age)")()
    val span = Alias(
      Multiply(Floor(Divide(UnresolvedAttribute("age"), Literal(5))), Literal(5)),
      "age_span")()
    val aggregatePlan = Aggregate(Seq(span), Seq(aggregateExpressions, span), tableRelation)
    val sortedPlan: LogicalPlan =
      Sort(Seq(SortOrder(UnresolvedAttribute("age"), Ascending)), global = true, aggregatePlan)
    val expectedPlan = Project(star, sortedPlan)

    comparePlans(expectedPlan, logPlan, false)
  }

  test("test number of flights sample stddev by airport with alias and limit") {
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(
      plan(
        pplParser,
        "source = table | stats stddev_samp(no_of_flights) as dev_samp_flights by airport | head 10"),
      context)
    // Define the expected logical plan
    val star = Seq(UnresolvedStar(None))
    val numberOfFlightsField = UnresolvedAttribute("no_of_flights")
    val airportField = UnresolvedAttribute("airport")
    val tableRelation = UnresolvedRelation(Seq("table"))

    val groupByAttributes = Seq(Alias(airportField, "airport")())
    val aggregateExpressions =
      Alias(
        UnresolvedFunction(Seq("STDDEV_SAMP"), Seq(numberOfFlightsField), isDistinct = false),
        "dev_samp_flights")()
    val airportAlias = Alias(airportField, "airport")()

    val aggregatePlan =
      Aggregate(groupByAttributes, Seq(aggregateExpressions, airportAlias), tableRelation)
    val planWithLimit = GlobalLimit(Literal(10), LocalLimit(Literal(10), aggregatePlan))
    val expectedPlan = Project(star, planWithLimit)

    comparePlans(expectedPlan, logPlan, false)
  }

  test("test price population stddev group by product sorted") {
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(
      plan(pplParser, "source = table | stats stddev_pop(price) by product | sort product"),
      context)
    val star = Seq(UnresolvedStar(None))
    val priceField = UnresolvedAttribute("price")
    val productField = UnresolvedAttribute("product")
    val tableRelation = UnresolvedRelation(Seq("table"))

    val groupByAttributes = Seq(Alias(productField, "product")())
    val aggregateExpressions =
      Alias(
        UnresolvedFunction(Seq("STDDEV_POP"), Seq(priceField), isDistinct = false),
        "stddev_pop(price)")()
    val productAlias = Alias(productField, "product")()

    val aggregatePlan =
      Aggregate(groupByAttributes, Seq(aggregateExpressions, productAlias), tableRelation)
    val sortedPlan: LogicalPlan =
      Sort(Seq(SortOrder(productField, Ascending)), global = true, aggregatePlan)
    val expectedPlan = Project(star, sortedPlan)

    comparePlans(expectedPlan, logPlan, false)
  }

  test("test price population stddev with alias and filter") {
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(
      plan(
        pplParser,
        "source = table category = 'vegetable' | stats stddev_pop(price) as dev_pop"),
      context)
    val star = Seq(UnresolvedStar(None))
    val categoryField = UnresolvedAttribute("category")
    val priceField = UnresolvedAttribute("price")
    val tableRelation = UnresolvedRelation(Seq("table"))

    val aggregateExpressions = Seq(
      Alias(
        UnresolvedFunction(Seq("STDDEV_POP"), Seq(priceField), isDistinct = false),
        "dev_pop")())
    val filterExpr = EqualTo(categoryField, Literal("vegetable"))
    val filterPlan = Filter(filterExpr, tableRelation)
    val aggregatePlan = Aggregate(Seq(), aggregateExpressions, filterPlan)
    val expectedPlan = Project(star, aggregatePlan)

    comparePlans(expectedPlan, logPlan, false)
  }

  test("test age population stddev by span of interval of 5 years query with sort ") {
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(
      plan(
        pplParser,
        "source = table | stats stddev_pop(age) by span(age, 5) as age_span | sort age"),
      context)
    // Define the expected logical plan
    val star = Seq(UnresolvedStar(None))
    val ageField = UnresolvedAttribute("age")
    val tableRelation = UnresolvedRelation(Seq("table"))

    val aggregateExpressions =
      Alias(
        UnresolvedFunction(Seq("STDDEV_POP"), Seq(ageField), isDistinct = false),
        "stddev_pop(age)")()
    val span = Alias(
      Multiply(Floor(Divide(UnresolvedAttribute("age"), Literal(5))), Literal(5)),
      "age_span")()
    val aggregatePlan = Aggregate(Seq(span), Seq(aggregateExpressions, span), tableRelation)
    val sortedPlan: LogicalPlan =
      Sort(Seq(SortOrder(UnresolvedAttribute("age"), Ascending)), global = true, aggregatePlan)
    val expectedPlan = Project(star, sortedPlan)

    comparePlans(expectedPlan, logPlan, false)
  }

  test("test number of flights population stddev by airport with alias and limit") {
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(
      plan(
        pplParser,
        "source = table | stats stddev_pop(no_of_flights) as dev_pop_flights by airport | head 50"),
      context)
    // Define the expected logical plan
    val star = Seq(UnresolvedStar(None))
    val numberOfFlightsField = UnresolvedAttribute("no_of_flights")
    val airportField = UnresolvedAttribute("airport")
    val tableRelation = UnresolvedRelation(Seq("table"))

    val groupByAttributes = Seq(Alias(airportField, "airport")())
    val aggregateExpressions =
      Alias(
        UnresolvedFunction(Seq("STDDEV_POP"), Seq(numberOfFlightsField), isDistinct = false),
        "dev_pop_flights")()
    val airportAlias = Alias(airportField, "airport")()

    val aggregatePlan =
      Aggregate(groupByAttributes, Seq(aggregateExpressions, airportAlias), tableRelation)
    val planWithLimit = GlobalLimit(Literal(50), LocalLimit(Literal(50), aggregatePlan))
    val expectedPlan = Project(star, planWithLimit)

    comparePlans(expectedPlan, logPlan, false)
  }

  test("test price 50th percentile group by product sorted") {
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(
      plan(pplParser, "source = table | stats percentile(price, 50) by product | sort product"),
      context)
    val star = Seq(UnresolvedStar(None))
    val priceField = UnresolvedAttribute("price")
    val productField = UnresolvedAttribute("product")
    val percentage = Literal(0.5)
    val tableRelation = UnresolvedRelation(Seq("table"))

    val groupByAttributes = Seq(Alias(productField, "product")())
    val aggregateExpressions =
      Alias(
        UnresolvedFunction(Seq("PERCENTILE"), Seq(priceField, percentage), isDistinct = false),
        "percentile(price, 50)")()
    val productAlias = Alias(productField, "product")()

    val aggregatePlan =
      Aggregate(groupByAttributes, Seq(aggregateExpressions, productAlias), tableRelation)
    val sortedPlan: LogicalPlan =
      Sort(Seq(SortOrder(productField, Ascending)), global = true, aggregatePlan)
    val expectedPlan = Project(star, sortedPlan)

    comparePlans(expectedPlan, logPlan, false)
  }

  test("test price 20th percentile with alias and filter") {
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(
      plan(
        pplParser,
        "source = table category = 'vegetable' | stats percentile(price, 20) as price_20_percentile"),
      context)
    val star = Seq(UnresolvedStar(None))
    val categoryField = UnresolvedAttribute("category")
    val priceField = UnresolvedAttribute("price")
    val percentage = Literal(0.2)
    val tableRelation = UnresolvedRelation(Seq("table"))

    val aggregateExpressions = Seq(
      Alias(
        UnresolvedFunction(Seq("PERCENTILE"), Seq(priceField, percentage), isDistinct = false),
        "price_20_percentile")())
    val filterExpr = EqualTo(categoryField, Literal("vegetable"))
    val filterPlan = Filter(filterExpr, tableRelation)
    val aggregatePlan = Aggregate(Seq(), aggregateExpressions, filterPlan)
    val expectedPlan = Project(star, aggregatePlan)

    comparePlans(expectedPlan, logPlan, false)
  }

  test("test age 40th percentile by span of interval of 5 years query with sort ") {
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(
      plan(
        pplParser,
        "source = table | stats percentile(age, 40) by span(age, 5) as age_span | sort age"),
      context)
    // Define the expected logical plan
    val star = Seq(UnresolvedStar(None))
    val ageField = UnresolvedAttribute("age")
    val percentage = Literal(0.4)
    val tableRelation = UnresolvedRelation(Seq("table"))

    val aggregateExpressions =
      Alias(
        UnresolvedFunction(Seq("PERCENTILE"), Seq(ageField, percentage), isDistinct = false),
        "percentile(age, 40)")()
    val span = Alias(
      Multiply(Floor(Divide(UnresolvedAttribute("age"), Literal(5))), Literal(5)),
      "age_span")()
    val aggregatePlan = Aggregate(Seq(span), Seq(aggregateExpressions, span), tableRelation)
    val sortedPlan: LogicalPlan =
      Sort(Seq(SortOrder(UnresolvedAttribute("age"), Ascending)), global = true, aggregatePlan)
    val expectedPlan = Project(star, sortedPlan)

    comparePlans(expectedPlan, logPlan, false)
  }

  test("test sum number of flights by airport and calculate 30th percentile with aliases") {
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(
      plan(
        pplParser,
        "source = table | stats sum(no_of_flights) as flights_count by airport | stats percentile(flights_count, 30) as percentile_30"),
      context)
    // Define the expected logical plan
    val star = Seq(UnresolvedStar(None))
    val numberOfFlightsField = UnresolvedAttribute("no_of_flights")
    val airportField = UnresolvedAttribute("airport")
    val percentage = Literal(0.3)
    val flightsCountField = UnresolvedAttribute("flights_count")
    val tableRelation = UnresolvedRelation(Seq("table"))

    val airportAlias = Alias(airportField, "airport")()
    val sumAggregateExpressions =
      Alias(
        UnresolvedFunction(Seq("SUM"), Seq(numberOfFlightsField), isDistinct = false),
        "flights_count")()
    val sumGroupByAttributes = Seq(Alias(airportField, "airport")())
    val sumAggregatePlan =
      Aggregate(sumGroupByAttributes, Seq(sumAggregateExpressions, airportAlias), tableRelation)

    val percentileAggregateExpressions =
      Alias(
        UnresolvedFunction(
          Seq("PERCENTILE"),
          Seq(flightsCountField, percentage),
          isDistinct = false),
        "percentile_30")()
    val percentileAggregatePlan =
      Aggregate(Seq(), Seq(percentileAggregateExpressions), sumAggregatePlan)
    val expectedPlan = Project(star, percentileAggregatePlan)

    comparePlans(expectedPlan, logPlan, false)
  }

  test(
    "test sum number of flights by airport and calculate 30th percentile approx with aliases") {
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(
      plan(
        pplParser,
        "source = table | stats sum(no_of_flights) as flights_count by airport | stats percentile_approx(flights_count, 30) as percentile_approx_30"),
      context)
    // Define the expected logical plan
    val star = Seq(UnresolvedStar(None))
    val numberOfFlightsField = UnresolvedAttribute("no_of_flights")
    val airportField = UnresolvedAttribute("airport")
    val percentage = Literal(0.3)
    val flightsCountField = UnresolvedAttribute("flights_count")
    val tableRelation = UnresolvedRelation(Seq("table"))

    val airportAlias = Alias(airportField, "airport")()
    val sumAggregateExpressions =
      Alias(
        UnresolvedFunction(Seq("SUM"), Seq(numberOfFlightsField), isDistinct = false),
        "flights_count")()
    val sumGroupByAttributes = Seq(Alias(airportField, "airport")())
    val sumAggregatePlan =
      Aggregate(sumGroupByAttributes, Seq(sumAggregateExpressions, airportAlias), tableRelation)

    val percentileAggregateExpressions =
      Alias(
        UnresolvedFunction(
          Seq("PERCENTILE_APPROX"),
          Seq(flightsCountField, percentage),
          isDistinct = false),
        "percentile_approx_30")()
    val percentileAggregatePlan =
      Aggregate(Seq(), Seq(percentileAggregateExpressions), sumAggregatePlan)
    val expectedPlan = Project(star, percentileAggregatePlan)

    comparePlans(expectedPlan, logPlan, false)
  }

  test("test distinct count product group by brand sorted") {
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(
      plan(pplParser, "source = table | stats distinct_count(product) by brand | sort brand"),
      context)
    val star = Seq(UnresolvedStar(None))
    val brandField = UnresolvedAttribute("brand")
    val productField = UnresolvedAttribute("product")
    val tableRelation = UnresolvedRelation(Seq("table"))

    val groupByAttributes = Seq(Alias(brandField, "brand")())
    val aggregateExpressions =
      Alias(
        UnresolvedFunction(Seq("COUNT"), Seq(productField), isDistinct = true),
        "distinct_count(product)")()
    val brandAlias = Alias(brandField, "brand")()

    val aggregatePlan =
      Aggregate(groupByAttributes, Seq(aggregateExpressions, brandAlias), tableRelation)
    val sortedPlan: LogicalPlan =
      Sort(Seq(SortOrder(brandField, Ascending)), global = true, aggregatePlan)
    val expectedPlan = Project(star, sortedPlan)

    comparePlans(expectedPlan, logPlan, false)
  }

  test("test approx distinct count product group by brand sorted") {
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(
      plan(
        pplParser,
        "source = table | stats distinct_count_approx(product) by brand | sort brand"),
      context)
    val star = Seq(UnresolvedStar(None))
    val brandField = UnresolvedAttribute("brand")
    val productField = UnresolvedAttribute("product")
    val tableRelation = UnresolvedRelation(Seq("table"))

    val groupByAttributes = Seq(Alias(brandField, "brand")())
    val aggregateExpressions =
      Alias(
        UnresolvedFunction(Seq("APPROX_COUNT_DISTINCT"), Seq(productField), isDistinct = true),
        "distinct_count_approx(product)")()
    val brandAlias = Alias(brandField, "brand")()

    val aggregatePlan =
      Aggregate(groupByAttributes, Seq(aggregateExpressions, brandAlias), tableRelation)
    val sortedPlan: LogicalPlan =
      Sort(Seq(SortOrder(brandField, Ascending)), global = true, aggregatePlan)
    val expectedPlan = Project(star, sortedPlan)

    comparePlans(expectedPlan, logPlan, false)
  }

  test("test distinct count product with alias and filter") {
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(
      plan(pplParser, "source = table price > 100 | stats distinct_count(product) as dc_product"),
      context)
    val star = Seq(UnresolvedStar(None))
    val productField = UnresolvedAttribute("product")
    val priceField = UnresolvedAttribute("price")
    val tableRelation = UnresolvedRelation(Seq("table"))

    val aggregateExpressions = Seq(
      Alias(
        UnresolvedFunction(Seq("COUNT"), Seq(productField), isDistinct = true),
        "dc_product")())
    val filterExpr = GreaterThan(priceField, Literal(100))
    val filterPlan = Filter(filterExpr, tableRelation)
    val aggregatePlan = Aggregate(Seq(), aggregateExpressions, filterPlan)
    val expectedPlan = Project(star, aggregatePlan)

    comparePlans(expectedPlan, logPlan, false)
  }

  test("test distinct count age by span of interval of 10 years query with sort ") {
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(
      plan(
        pplParser,
        "source = table | stats distinct_count(age) by span(age, 10) as age_span | sort age"),
      context)
    // Define the expected logical plan
    val star = Seq(UnresolvedStar(None))
    val ageField = UnresolvedAttribute("age")
    val tableRelation = UnresolvedRelation(Seq("table"))

    val aggregateExpressions =
      Alias(
        UnresolvedFunction(Seq("COUNT"), Seq(ageField), isDistinct = true),
        "distinct_count(age)")()
    val span = Alias(
      Multiply(Floor(Divide(UnresolvedAttribute("age"), Literal(10))), Literal(10)),
      "age_span")()
    val aggregatePlan = Aggregate(Seq(span), Seq(aggregateExpressions, span), tableRelation)
    val sortedPlan: LogicalPlan =
      Sort(Seq(SortOrder(UnresolvedAttribute("age"), Ascending)), global = true, aggregatePlan)
    val expectedPlan = Project(star, sortedPlan)

    comparePlans(expectedPlan, logPlan, false)
  }

  test(
    "test distinct count age by span of interval of 10 years query with sort using approximation ") {
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(
      plan(
        pplParser,
        "source = table | stats distinct_count_approx(age) by span(age, 10) as age_span | sort age"),
      context)
    // Define the expected logical plan
    val star = Seq(UnresolvedStar(None))
    val ageField = UnresolvedAttribute("age")
    val tableRelation = UnresolvedRelation(Seq("table"))

    val aggregateExpressions =
      Alias(
        UnresolvedFunction(Seq("APPROX_COUNT_DISTINCT"), Seq(ageField), isDistinct = true),
        "distinct_count_approx(age)")()
    val span = Alias(
      Multiply(Floor(Divide(UnresolvedAttribute("age"), Literal(10))), Literal(10)),
      "age_span")()
    val aggregatePlan = Aggregate(Seq(span), Seq(aggregateExpressions, span), tableRelation)
    val sortedPlan: LogicalPlan =
      Sort(Seq(SortOrder(UnresolvedAttribute("age"), Ascending)), global = true, aggregatePlan)
    val expectedPlan = Project(star, sortedPlan)

    comparePlans(expectedPlan, logPlan, false)
  }

  test("test distinct count status by week window and group by status with limit") {
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(
      plan(
        pplParser,
        "source = table | stats distinct_count(status) by span(@timestamp, 1w) as status_count_by_week, status | head 100"),
      context)
    // Define the expected logical plan
    val star = Seq(UnresolvedStar(None))
    val status = Alias(UnresolvedAttribute("status"), "status")()
    val statusCount = UnresolvedAttribute("status")
    val table = UnresolvedRelation(Seq("table"))

    val windowExpression = Alias(
      TimeWindow(
        UnresolvedAttribute("`@timestamp`"),
        TimeWindow.parseExpression(Literal("1 week")),
        TimeWindow.parseExpression(Literal("1 week")),
        0),
      "status_count_by_week")()

    val aggregateExpressions =
      Alias(
        UnresolvedFunction(Seq("COUNT"), Seq(statusCount), isDistinct = true),
        "distinct_count(status)")()
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
    "test distinct count status by week window and group by status with limit using approximation") {
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(
      plan(
        pplParser,
        "source = table | stats distinct_count_approx(status) by span(@timestamp, 1w) as status_count_by_week, status | head 100"),
      context)
    // Define the expected logical plan
    val star = Seq(UnresolvedStar(None))
    val status = Alias(UnresolvedAttribute("status"), "status")()
    val statusCount = UnresolvedAttribute("status")
    val table = UnresolvedRelation(Seq("table"))

    val windowExpression = Alias(
      TimeWindow(
        UnresolvedAttribute("`@timestamp`"),
        TimeWindow.parseExpression(Literal("1 week")),
        TimeWindow.parseExpression(Literal("1 week")),
        0),
      "status_count_by_week")()

    val aggregateExpressions =
      Alias(
        UnresolvedFunction(Seq("APPROX_COUNT_DISTINCT"), Seq(statusCount), isDistinct = true),
        "distinct_count_approx(status)")()
    val aggregatePlan = Aggregate(
      Seq(status, windowExpression),
      Seq(aggregateExpressions, status, windowExpression),
      table)
    val planWithLimit = GlobalLimit(Literal(100), LocalLimit(Literal(100), aggregatePlan))
    val expectedPlan = Project(star, planWithLimit)
    // Compare the two plans
    comparePlans(expectedPlan, logPlan, false)
  }

  test("multiple stats - test average price and average age") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(pplParser, "source = table | stats avg(price) | stats avg(age)"),
        context)
    val star = Seq(UnresolvedStar(None))

    val priceField = UnresolvedAttribute("price")
    val ageField = UnresolvedAttribute("age")
    val tableRelation = UnresolvedRelation(Seq("table"))
    val aggregateExpressions1 = Seq(
      Alias(UnresolvedFunction(Seq("AVG"), Seq(priceField), isDistinct = false), "avg(price)")())
    val aggregatePlan1 = Aggregate(Seq(), aggregateExpressions1, tableRelation)
    val aggregateExpressions2 =
      Seq(Alias(UnresolvedFunction(Seq("AVG"), Seq(ageField), isDistinct = false), "avg(age)")())
    val aggregatePlan2 = Aggregate(Seq(), aggregateExpressions2, aggregatePlan1)
    val expectedPlan = Project(star, aggregatePlan2)

    comparePlans(expectedPlan, logPlan, false)
  }

  test("multiple stats - test average price and average age with Alias") {
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(
      plan(
        pplParser,
        "source = table | stats avg(price) as avg_price | stats avg(age) as avg_age"),
      context)
    val star = Seq(UnresolvedStar(None))

    val priceField = UnresolvedAttribute("price")
    val ageField = UnresolvedAttribute("age")
    val tableRelation = UnresolvedRelation(Seq("table"))
    val aggregateExpressions1 = Seq(
      Alias(UnresolvedFunction(Seq("AVG"), Seq(priceField), isDistinct = false), "avg_price")())
    val aggregatePlan1 = Aggregate(Seq(), aggregateExpressions1, tableRelation)
    val aggregateExpressions2 =
      Seq(Alias(UnresolvedFunction(Seq("AVG"), Seq(ageField), isDistinct = false), "avg_age")())
    val aggregatePlan2 = Aggregate(Seq(), aggregateExpressions2, aggregatePlan1)
    val expectedPlan = Project(star, aggregatePlan2)

    comparePlans(expectedPlan, logPlan, false)
  }

  test(
    "multiple stats - test average price group by product and average age by span of interval of 10 years") {
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(
      plan(
        pplParser,
        "source = table | stats avg(price) by product | stats avg(age) by span(age, 10) as age_span"),
      context)
    val star = Seq(UnresolvedStar(None))
    val productField = UnresolvedAttribute("product")
    val priceField = UnresolvedAttribute("price")
    val ageField = UnresolvedAttribute("age")
    val tableRelation = UnresolvedRelation(Seq("table"))

    val groupByAttributes = Seq(Alias(productField, "product")())
    val aggregateExpressions1 =
      Alias(UnresolvedFunction(Seq("AVG"), Seq(priceField), isDistinct = false), "avg(price)")()
    val productAlias = Alias(productField, "product")()

    val aggregatePlan1 =
      Aggregate(groupByAttributes, Seq(aggregateExpressions1, productAlias), tableRelation)

    val aggregateExpressions2 =
      Alias(UnresolvedFunction(Seq("AVG"), Seq(ageField), isDistinct = false), "avg(age)")()
    val span = Alias(
      Multiply(Floor(Divide(UnresolvedAttribute("age"), Literal(10))), Literal(10)),
      "age_span")()
    val aggregatePlan2 = Aggregate(Seq(span), Seq(aggregateExpressions2, span), aggregatePlan1)

    val expectedPlan = Project(star, aggregatePlan2)

    comparePlans(expectedPlan, logPlan, false)
  }

  test("multiple levels stats") {
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(
      plan(
        pplParser,
        "source = table | stats avg(response_time) as avg_response_time by host, service | stats avg(avg_response_time) as avg_host_response_time by service"),
      context)
    val star = Seq(UnresolvedStar(None))
    val hostField = UnresolvedAttribute("host")
    val serviceField = UnresolvedAttribute("service")
    val ageField = UnresolvedAttribute("age")
    val responseTimeField = UnresolvedAttribute("response_time")
    val tableRelation = UnresolvedRelation(Seq("table"))
    val hostAlias = Alias(hostField, "host")()
    val serviceAlias = Alias(serviceField, "service")()

    val groupByAttributes1 = Seq(Alias(hostField, "host")(), Alias(serviceField, "service")())
    val aggregateExpressions1 =
      Alias(
        UnresolvedFunction(Seq("AVG"), Seq(responseTimeField), isDistinct = false),
        "avg_response_time")()
    val responseTimeAlias = Alias(responseTimeField, "response_time")()
    val aggregatePlan1 =
      Aggregate(
        groupByAttributes1,
        Seq(aggregateExpressions1, hostAlias, serviceAlias),
        tableRelation)

    val avgResponseTimeField = UnresolvedAttribute("avg_response_time")
    val groupByAttributes2 = Seq(Alias(serviceField, "service")())
    val aggregateExpressions2 =
      Alias(
        UnresolvedFunction(Seq("AVG"), Seq(avgResponseTimeField), isDistinct = false),
        "avg_host_response_time")()

    val aggregatePlan2 =
      Aggregate(groupByAttributes2, Seq(aggregateExpressions2, serviceAlias), aggregatePlan1)

    val expectedPlan = Project(star, aggregatePlan2)

    comparePlans(expectedPlan, logPlan, false)
  }

  test("test count() as the last aggregator in stats clause") {
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(
      plan(
        pplParser,
        "source = table | eval a = 1 | stats sum(a) as sum, avg(a) as avg, count() as cnt"),
      context)
    val tableRelation = UnresolvedRelation(Seq("table"))
    val eval = Project(Seq(UnresolvedStar(None), Alias(Literal(1), "a")()), tableRelation)
    val sum =
      Alias(
        UnresolvedFunction(Seq("SUM"), Seq(UnresolvedAttribute("a")), isDistinct = false),
        "sum")()
    val avg =
      Alias(
        UnresolvedFunction(Seq("AVG"), Seq(UnresolvedAttribute("a")), isDistinct = false),
        "avg")()
    val count =
      Alias(
        UnresolvedFunction(Seq("COUNT"), Seq(UnresolvedStar(None)), isDistinct = false),
        "cnt")()
    val aggregate = Aggregate(Seq.empty, Seq(sum, avg, count), eval)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), aggregate)
    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }

  test("test count() as the last aggregator in stats by clause") {
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(
      plan(
        pplParser,
        "source = table | eval a = 1 | stats sum(a) as sum, avg(a) as avg, count() as cnt by country"),
      context)
    val tableRelation = UnresolvedRelation(Seq("table"))
    val eval = Project(Seq(UnresolvedStar(None), Alias(Literal(1), "a")()), tableRelation)
    val sum =
      Alias(
        UnresolvedFunction(Seq("SUM"), Seq(UnresolvedAttribute("a")), isDistinct = false),
        "sum")()
    val avg =
      Alias(
        UnresolvedFunction(Seq("AVG"), Seq(UnresolvedAttribute("a")), isDistinct = false),
        "avg")()
    val count =
      Alias(
        UnresolvedFunction(Seq("COUNT"), Seq(UnresolvedStar(None)), isDistinct = false),
        "cnt")()
    val grouping =
      Alias(UnresolvedAttribute("country"), "country")()
    val aggregate = Aggregate(Seq(grouping), Seq(sum, avg, count, grouping), eval)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), aggregate)
    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }

  test("test average price with backticks alias") {
    val expectedPlan = planTransformer.visit(
      plan(pplParser, "source = table | stats avg(price) as avg_price"),
      new CatalystPlanContext)
    val logPlan = planTransformer.visit(
      plan(pplParser, "source = table | stats avg(`price`) as `avg_price`"),
      new CatalystPlanContext)
    comparePlans(expectedPlan, logPlan, false)
  }
}
