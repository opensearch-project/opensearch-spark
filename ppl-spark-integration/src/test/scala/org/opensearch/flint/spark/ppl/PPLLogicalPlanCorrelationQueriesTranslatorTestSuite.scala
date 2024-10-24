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
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Ascending, Descending, Divide, EqualTo, Floor, GreaterThan, Literal, Multiply, NamedExpression, Or, SortOrder}
import org.apache.spark.sql.catalyst.plans.{FullOuter, Inner, PlanTest}
import org.apache.spark.sql.catalyst.plans.logical._

class PPLLogicalPlanCorrelationQueriesTranslatorTestSuite
    extends SparkFunSuite
    with PlanTest
    with LogicalPlanTestUtils
    with Matchers {

  private val planTransformer = new CatalystQueryPlanVisitor()
  private val pplParser = new PPLSyntaxParser()

  /** Test table and index name */
  private val testTable1 = "spark_catalog.default.flint_ppl_test1"
  private val testTable2 = "spark_catalog.default.flint_ppl_test2"

  test("create failing ppl correlation query - due to mismatch fields to mappings test") {
    val context = new CatalystPlanContext
    val thrown = intercept[IllegalStateException] {
      planTransformer.visit(
        plan(
          pplParser,
          s"""
             | source = $testTable1, $testTable2| correlate exact fields(name, country) scope(month, 1W) mapping($testTable1.name = $testTable2.name)
             | """.stripMargin),
        context)
    }
    assert(
      thrown.getMessage === "Correlation command was called with `fields` attribute having different elements from the 'mapping' attributes ")
  }

  test(
    "create failing ppl correlation query with no scope - due to mismatch fields to mappings test") {
    val context = new CatalystPlanContext
    val thrown = intercept[IllegalStateException] {
      planTransformer.visit(
        plan(
          pplParser,
          s"""
             | source = $testTable1, $testTable2| correlate exact fields(name, country) mapping($testTable1.name = $testTable2.name)
             | """.stripMargin),
        context)
    }
    assert(
      thrown.getMessage === "Correlation command was called with `fields` attribute having different elements from the 'mapping' attributes ")
  }

  test(
    "create failing ppl correlation query - due to mismatch correlation self type and source amount test") {
    val context = new CatalystPlanContext
    val thrown = intercept[IllegalStateException] {
      planTransformer.visit(
        plan(
          pplParser,
          s"""
             | source = $testTable1, $testTable2| correlate self fields(name, country) scope(month, 1W) mapping($testTable1.name = $testTable2.name)
             | """.stripMargin),
        context)
    }
    assert(
      thrown.getMessage === "Correlation command with `inner` type must have exactly on source table ")
  }

  test(
    "create failing ppl correlation query - due to mismatch correlation exact type and source amount test") {
    val context = new CatalystPlanContext
    val thrown = intercept[IllegalStateException] {
      planTransformer.visit(
        plan(
          pplParser,
          s"""
             | source = $testTable1| correlate approximate fields(name) scope(month, 1W) mapping($testTable1.name = $testTable1.inner_name)
             | """.stripMargin),
        context)
    }
    assert(
      thrown.getMessage === "Correlation command with `approximate` type must at least two different source tables ")
  }

  test(
    "create ppl correlation exact query with filters and two tables correlating on a single field test") {
    val context = new CatalystPlanContext
    val logicalPlan = planTransformer.visit(
      plan(
        pplParser,
        s"""
           | source = $testTable1, $testTable2 | where year = 2023 AND month = 4 | correlate exact fields(name) scope(month, 1W) mapping($testTable1.name = $testTable2.name)
           | """.stripMargin),
      context)

    // Define unresolved relations
    val table1 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test1"))
    val table2 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test2"))

    // Define filter expressions
    val filter1Expr = And(
      EqualTo(UnresolvedAttribute("year"), Literal(2023)),
      EqualTo(UnresolvedAttribute("month"), Literal(4)))
    val filter2Expr = And(
      EqualTo(UnresolvedAttribute("year"), Literal(2023)),
      EqualTo(UnresolvedAttribute("month"), Literal(4)))
    // Define subquery aliases
    val plan1 = Filter(filter1Expr, table1)
    val plan2 = Filter(filter2Expr, table2)

    // Define join condition
    val joinCondition =
      EqualTo(UnresolvedAttribute(s"$testTable1.name"), UnresolvedAttribute(s"$testTable2.name"))

    // Create Join plan
    val joinPlan = Join(plan1, plan2, Inner, Some(joinCondition), JoinHint.NONE)

    // Add the projection
    val expectedPlan = Project(Seq(UnresolvedStar(None)), joinPlan)

    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  test(
    "create ppl correlation approximate query with filters and two tables correlating on a single field test") {
    val context = new CatalystPlanContext
    val logicalPlan = planTransformer.visit(
      plan(
        pplParser,
        s"""
           | source = $testTable1, $testTable2| correlate approximate fields(name) scope(month, 1W) mapping($testTable1.name = $testTable2.name)
           | """.stripMargin),
      context)

    // Define unresolved relations
    val table1 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test1"))
    val table2 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test2"))
    // Define join condition
    val joinCondition =
      EqualTo(UnresolvedAttribute(s"$testTable1.name"), UnresolvedAttribute(s"$testTable2.name"))

    // Create Join plan
    val joinPlan = Join(table1, table2, FullOuter, Some(joinCondition), JoinHint.NONE)

    // Add the projection
    val expectedPlan = Project(Seq(UnresolvedStar(None)), joinPlan)

    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  test(
    "create ppl correlation approximate query with two tables correlating on a single field and not scope test") {
    val context = new CatalystPlanContext
    val logicalPlan = planTransformer.visit(
      plan(
        pplParser,
        s"""
           | source = $testTable1, $testTable2| correlate approximate fields(name) mapping($testTable1.name = $testTable2.name)
           | """.stripMargin),
      context)
    // Define unresolved relations
    val table1 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test1"))
    val table2 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test2"))
    // Define join condition
    val joinCondition =
      EqualTo(UnresolvedAttribute(s"$testTable1.name"), UnresolvedAttribute(s"$testTable2.name"))

    // Create Join plan
    val joinPlan = Join(table1, table2, FullOuter, Some(joinCondition), JoinHint.NONE)

    // Add the projection
    val expectedPlan = Project(Seq(UnresolvedStar(None)), joinPlan)

    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  test(
    "create ppl correlation query with with filters and two tables correlating on a two fields test") {
    val context = new CatalystPlanContext
    val logicalPlan = planTransformer.visit(
      plan(
        pplParser,
        s"""
           | source = $testTable1, $testTable2| where year = 2023 AND month = 4 | correlate exact fields(name, country) scope(month, 1W)
           | mapping($testTable1.name = $testTable2.name, $testTable1.country = $testTable2.country)
           | """.stripMargin),
      context)
    // Define unresolved relations
    val table1 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test1"))
    val table2 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test2"))

    // Define filter expressions
    val filter1Expr = And(
      EqualTo(UnresolvedAttribute("year"), Literal(2023)),
      EqualTo(UnresolvedAttribute("month"), Literal(4)))
    val filter2Expr = And(
      EqualTo(UnresolvedAttribute("year"), Literal(2023)),
      EqualTo(UnresolvedAttribute("month"), Literal(4)))
    // Define subquery aliases
    val plan1 = Filter(filter1Expr, table1)
    val plan2 = Filter(filter2Expr, table2)

    // Define join condition
    val joinCondition =
      And(
        EqualTo(
          UnresolvedAttribute(s"$testTable1.name"),
          UnresolvedAttribute(s"$testTable2.name")),
        EqualTo(
          UnresolvedAttribute(s"$testTable1.country"),
          UnresolvedAttribute(s"$testTable2.country")))

    // Create Join plan
    val joinPlan = Join(plan1, plan2, Inner, Some(joinCondition), JoinHint.NONE)

    // Add the projection
    val expectedPlan = Project(Seq(UnresolvedStar(None)), joinPlan)

    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  test(
    "create ppl correlation query with two tables correlating on a two fields and disjoint filters test") {
    val context = new CatalystPlanContext
    val logicalPlan = planTransformer.visit(
      plan(
        pplParser,
        s"""
           | source = $testTable1, $testTable2| where year = 2023 AND month = 4 AND $testTable2.salary > 100000 | correlate exact fields(name, country) scope(month, 1W)
           | mapping($testTable1.name = $testTable2.name, $testTable1.country = $testTable2.country)
           | """.stripMargin),
      context)
    // Define unresolved relations
    val table1 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test1"))
    val table2 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test2"))

    // Define filter expressions
    val filter1Expr = And(
      EqualTo(UnresolvedAttribute("year"), Literal(2023)),
      EqualTo(UnresolvedAttribute("month"), Literal(4)))
    val filter2Expr = And(
      And(
        EqualTo(UnresolvedAttribute("year"), Literal(2023)),
        EqualTo(UnresolvedAttribute("month"), Literal(4))),
      GreaterThan(UnresolvedAttribute(s"$testTable2.salary"), Literal(100000)))
    // Define subquery aliases
    val plan1 = Filter(filter1Expr, table1)
    val plan2 = Filter(filter2Expr, table2)

    // Define join condition
    val joinCondition =
      And(
        EqualTo(
          UnresolvedAttribute(s"$testTable1.name"),
          UnresolvedAttribute(s"$testTable2.name")),
        EqualTo(
          UnresolvedAttribute(s"$testTable1.country"),
          UnresolvedAttribute(s"$testTable2.country")))

    // Create Join plan
    val joinPlan = Join(plan1, plan2, Inner, Some(joinCondition), JoinHint.NONE)

    // Add the projection
    val expectedPlan = Project(Seq(UnresolvedStar(None)), joinPlan)

    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  test(
    "create ppl correlation (exact) query with two tables correlating by name and group by avg salary by age span (10 years bucket) test") {
    val context = new CatalystPlanContext
    val logicalPlan = planTransformer.visit(
      plan(
        pplParser,
        s"""
           | source = $testTable1, $testTable2| correlate exact fields(name) scope(month, 1W)
           | mapping($testTable1.name = $testTable2.name) |
           | stats avg(salary) by span(age, 10) as age_span
           | """.stripMargin),
      context)
    // Define unresolved relations
    val table1 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test1"))
    val table2 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test2"))

    // Define join condition
    val joinCondition =
      EqualTo(UnresolvedAttribute(s"$testTable1.name"), UnresolvedAttribute(s"$testTable2.name"))

    // Create Join plan
    val joinPlan = Join(table1, table2, Inner, Some(joinCondition), JoinHint.NONE)

    val salaryField = UnresolvedAttribute("salary")
    val star = Seq(UnresolvedStar(None))
    val aggregateExpressions =
      Alias(UnresolvedFunction(Seq("AVG"), Seq(salaryField), isDistinct = false), "avg(salary)")()
    val span = Alias(
      Multiply(Floor(Divide(UnresolvedAttribute("age"), Literal(10))), Literal(10)),
      "age_span")()
    val aggregatePlan =
      Aggregate(Seq(span), Seq(aggregateExpressions, span), joinPlan)
    // Add the projection
    val expectedPlan = Project(star, aggregatePlan)

    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  test(
    "create ppl correlation (exact) query with two tables correlating by name and group by avg salary by age span (10 years bucket) and country test") {
    val context = new CatalystPlanContext
    val logicalPlan = planTransformer.visit(
      plan(
        pplParser,
        s"""
           | source = $testTable1, $testTable2| correlate exact fields(name) scope(month, 1W)
           | mapping($testTable1.name = $testTable2.name) |
           | stats avg(salary) by span(age, 10) as age_span, $testTable2.country
           | """.stripMargin),
      context)
    // Define unresolved relations
    val table1 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test1"))
    val table2 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test2"))

    // Define join condition
    val joinCondition =
      EqualTo(UnresolvedAttribute(s"$testTable1.name"), UnresolvedAttribute(s"$testTable2.name"))

    // Create Join plan
    val joinPlan = Join(table1, table2, Inner, Some(joinCondition), JoinHint.NONE)

    val salaryField = UnresolvedAttribute("salary")
    val countryField = UnresolvedAttribute(s"$testTable2.country")
    val countryAlias = Alias(countryField, s"$testTable2.country")()
    val star = Seq(UnresolvedStar(None))
    val aggregateExpressions =
      Alias(UnresolvedFunction(Seq("AVG"), Seq(salaryField), isDistinct = false), "avg(salary)")()
    val span = Alias(
      Multiply(Floor(Divide(UnresolvedAttribute("age"), Literal(10))), Literal(10)),
      "age_span")()
    val aggregatePlan =
      Aggregate(Seq(countryAlias, span), Seq(aggregateExpressions, countryAlias, span), joinPlan)
    // Add the projection
    val expectedPlan = Project(star, aggregatePlan)

    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  test(
    "create ppl correlation (exact) query with two tables correlating by name,country and group by avg salary by age span (10 years bucket) with country filter test") {
    val context = new CatalystPlanContext
    val logicalPlan = planTransformer.visit(
      plan(
        pplParser,
        s"""
           | source = $testTable1, $testTable2| where country = 'USA' OR country = 'England' |
           | correlate exact fields(name) scope(month, 1W) mapping($testTable1.name = $testTable2.name) |
           | stats avg(salary) by span(age, 10) as age_span, $testTable2.country
           | """.stripMargin),
      context)
    // Define unresolved relations
    val table1 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test1"))
    val table2 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test2"))

    // Define filter expressions
    val filter1Expr = Or(
      EqualTo(UnresolvedAttribute("country"), Literal("USA")),
      EqualTo(UnresolvedAttribute("country"), Literal("England")))
    val filter2Expr = Or(
      EqualTo(UnresolvedAttribute("country"), Literal("USA")),
      EqualTo(UnresolvedAttribute("country"), Literal("England")))
    // Define subquery aliases
    val plan1 = Filter(filter1Expr, table1)
    val plan2 = Filter(filter2Expr, table2)

    // Define join condition
    val joinCondition =
      EqualTo(UnresolvedAttribute(s"$testTable1.name"), UnresolvedAttribute(s"$testTable2.name"))

    // Create Join plan
    val joinPlan = Join(plan1, plan2, Inner, Some(joinCondition), JoinHint.NONE)

    val salaryField = UnresolvedAttribute("salary")
    val countryField = UnresolvedAttribute(s"$testTable2.country")
    val countryAlias = Alias(countryField, s"$testTable2.country")()
    val star = Seq(UnresolvedStar(None))
    val aggregateExpressions =
      Alias(UnresolvedFunction(Seq("AVG"), Seq(salaryField), isDistinct = false), "avg(salary)")()
    val span = Alias(
      Multiply(Floor(Divide(UnresolvedAttribute("age"), Literal(10))), Literal(10)),
      "age_span")()
    val aggregatePlan =
      Aggregate(Seq(countryAlias, span), Seq(aggregateExpressions, countryAlias, span), joinPlan)
    // Add the projection
    val expectedPlan = Project(star, aggregatePlan)

    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  test(
    "create ppl correlation (exact) query with two tables correlating by name,country and group by avg salary by age span (10 years bucket) with country filter without scope test") {
    val context = new CatalystPlanContext
    val logicalPlan = planTransformer.visit(
      plan(
        pplParser,
        s"""
           | source = $testTable1, $testTable2| where country = 'USA' OR country = 'England' |
           | correlate exact fields(name) mapping($testTable1.name = $testTable2.name) |
           | stats avg(salary) by span(age, 10) as age_span, $testTable2.country
           | """.stripMargin),
      context)

    // Define unresolved relations
    val table1 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test1"))
    val table2 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test2"))

    // Define filter expressions
    val filter1Expr = Or(
      EqualTo(UnresolvedAttribute("country"), Literal("USA")),
      EqualTo(UnresolvedAttribute("country"), Literal("England")))
    val filter2Expr = Or(
      EqualTo(UnresolvedAttribute("country"), Literal("USA")),
      EqualTo(UnresolvedAttribute("country"), Literal("England")))
    // Define subquery aliases
    val plan1 = Filter(filter1Expr, table1)
    val plan2 = Filter(filter2Expr, table2)

    // Define join condition
    val joinCondition =
      EqualTo(UnresolvedAttribute(s"$testTable1.name"), UnresolvedAttribute(s"$testTable2.name"))

    // Create Join plan
    val joinPlan = Join(plan1, plan2, Inner, Some(joinCondition), JoinHint.NONE)

    val salaryField = UnresolvedAttribute("salary")
    val countryField = UnresolvedAttribute(s"$testTable2.country")
    val countryAlias = Alias(countryField, s"$testTable2.country")()
    val star = Seq(UnresolvedStar(None))
    val aggregateExpressions =
      Alias(UnresolvedFunction(Seq("AVG"), Seq(salaryField), isDistinct = false), "avg(salary)")()
    val span = Alias(
      Multiply(Floor(Divide(UnresolvedAttribute("age"), Literal(10))), Literal(10)),
      "age_span")()
    val aggregatePlan =
      Aggregate(Seq(countryAlias, span), Seq(aggregateExpressions, countryAlias, span), joinPlan)
    // Add the projection
    val expectedPlan = Project(star, aggregatePlan)

    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  test(
    "create ppl correlation (approximate) query with two tables correlating by name,country and group by avg salary by age span (10 years bucket) test") {
    val context = new CatalystPlanContext
    val logicalPlan = planTransformer.visit(
      plan(
        pplParser,
        s"""
           | source = $testTable1, $testTable2| correlate approximate fields(name, country) scope(month, 1W)
           | mapping($testTable1.name = $testTable2.name, $testTable1.country = $testTable2.country) |
           | stats avg(salary) by span(age, 10) as age_span, $testTable2.country | sort - age_span | head 5
           | """.stripMargin),
      context)

    // Define unresolved relations
    val table1 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test1"))
    val table2 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test2"))

    // Define join condition - according to the correlation (approximate) type
    val joinCondition =
      Or(
        EqualTo(
          UnresolvedAttribute(s"$testTable1.name"),
          UnresolvedAttribute(s"$testTable2.name")),
        EqualTo(
          UnresolvedAttribute(s"$testTable1.country"),
          UnresolvedAttribute(s"$testTable2.country")))

    // Create Join plan
    val joinPlan = Join(table1, table2, FullOuter, Some(joinCondition), JoinHint.NONE)

    val salaryField = UnresolvedAttribute("salary")
    val countryField = UnresolvedAttribute(s"$testTable2.country")
    val countryAlias = Alias(countryField, s"$testTable2.country")()
    val star = Seq(UnresolvedStar(None))
    val aggregateExpressions =
      Alias(UnresolvedFunction(Seq("AVG"), Seq(salaryField), isDistinct = false), "avg(salary)")()
    val span = Alias(
      Multiply(Floor(Divide(UnresolvedAttribute("age"), Literal(10))), Literal(10)),
      "age_span")()
    val aggregatePlan =
      Aggregate(Seq(countryAlias, span), Seq(aggregateExpressions, countryAlias, span), joinPlan)

    // sort by age_span
    val sortedPlan: LogicalPlan =
      Sort(
        Seq(SortOrder(UnresolvedAttribute("age_span"), Descending)),
        global = true,
        aggregatePlan)

    val limitPlan = Limit(Literal(5), sortedPlan)
    val expectedPlan = Project(star, limitPlan)

    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }
}
