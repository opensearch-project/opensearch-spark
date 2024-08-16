/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Ascending, Descending, Divide, EqualTo, Floor, GreaterThan, Literal, Multiply, Or, SortOrder}
import org.apache.spark.sql.catalyst.plans.{FullOuter, Inner}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.logical.JoinHint.NONE
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.streaming.StreamTest

class FlintSparkPPLCorrelationITSuite
    extends QueryTest
    with LogicalPlanTestUtils
    with FlintPPLSuite
    with StreamTest {

  /** Test table and index name */
  private val testTable1 = "spark_catalog.default.flint_ppl_test1"
  private val testTable2 = "spark_catalog.default.flint_ppl_test2"
  private val testTable3 = "spark_catalog.default.flint_ppl_test3"

  override def beforeAll(): Unit = {
    super.beforeAll()
    // Create test tables
    createPartitionedStateCountryTable(testTable1)
    // Update data insertion
    sql(s"""
         | INSERT INTO $testTable1
         | PARTITION (year=2023, month=4)
         | VALUES ('Jim', 27,  'B.C', 'Canada'),
         |        ('Peter', 57,  'B.C', 'Canada'),
         |        ('Rick', 70,  'B.C', 'Canada'),
         |        ('David', 40,  'Washington', 'USA')
         | """.stripMargin)

    createOccupationTable(testTable2)
    createHobbiesTable(testTable3)
  }

  protected override def afterEach(): Unit = {
    super.afterEach()
    // Stop all streaming jobs if any
    spark.streams.active.foreach { job =>
      job.stop()
      job.awaitTermination()
    }
  }

  test("create failing ppl correlation query - due to mismatch fields to mappings test") {
    val thrown = intercept[IllegalStateException] {
      val frame = sql(s"""
           | source = $testTable1, $testTable2| correlate exact fields(name, country) scope(month, 1W) mapping($testTable1.name = $testTable2.name)
           | """.stripMargin)
    }
    assert(
      thrown.getMessage === "Correlation command was called with `fields` attribute having different elements from the 'mapping' attributes ")
  }
  test(
    "create failing ppl correlation query with no scope - due to mismatch fields to mappings test") {
    val thrown = intercept[IllegalStateException] {
      val frame = sql(s"""
           | source = $testTable1, $testTable2| correlate exact fields(name, country) mapping($testTable1.name = $testTable2.name)
           | """.stripMargin)
    }
    assert(
      thrown.getMessage === "Correlation command was called with `fields` attribute having different elements from the 'mapping' attributes ")
  }

  test(
    "create failing ppl correlation query - due to mismatch correlation self type and source amount test") {
    val thrown = intercept[IllegalStateException] {
      val frame = sql(s"""
           | source = $testTable1, $testTable2| correlate self fields(name, country) scope(month, 1W) mapping($testTable1.name = $testTable2.name)
           | """.stripMargin)
    }
    assert(
      thrown.getMessage === "Correlation command with `inner` type must have exactly on source table ")
  }

  test(
    "create failing ppl correlation query - due to mismatch correlation exact type and source amount test") {
    val thrown = intercept[IllegalStateException] {
      val frame = sql(s"""
           | source = $testTable1 | correlate approximate fields(name) scope(month, 1W) mapping($testTable1.name = $testTable1.inner_name)
           | """.stripMargin)
    }
    assert(
      thrown.getMessage === "Correlation command with `approximate` type must at least two different source tables ")
  }

  test(
    "create ppl correlation exact query with filters and two tables correlating on a single field test") {
    val joinQuery =
      s"""
         | SELECT a.name, a.age, a.state, a.country, b.occupation, b.salary
         | FROM $testTable1 AS a
         | JOIN $testTable2 AS b
         | ON a.name = b.name
         | WHERE a.year = 2023 AND a.month = 4 AND b.year = 2023 AND b.month = 4
         |""".stripMargin

    val result = spark.sql(joinQuery)
    result.show()

    val frame = sql(s"""
         | source = $testTable1, $testTable2| where year = 2023 AND month = 4 | correlate exact fields(name) scope(month, 1W) mapping($testTable1.name = $testTable2.name)
         | """.stripMargin)
    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(
      Row(
        "Jake",
        70,
        "California",
        "USA",
        2023,
        4,
        "Jake",
        "Engineer",
        "England",
        100000,
        2023,
        4),
      Row("Hello", 30, "New York", "USA", 2023, 4, "Hello", "Artist", "USA", 70000, 2023, 4),
      Row("John", 25, "Ontario", "Canada", 2023, 4, "John", "Doctor", "Canada", 120000, 2023, 4),
      Row("David", 40, "Washington", "USA", 2023, 4, "David", "Unemployed", "Canada", 0, 2023, 4),
      Row("David", 40, "Washington", "USA", 2023, 4, "David", "Doctor", "USA", 120000, 2023, 4),
      Row("Jane", 20, "Quebec", "Canada", 2023, 4, "Jane", "Scientist", "Canada", 90000, 2023, 4))

    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    // Compare the results
    assert(results.sorted.sameElements(expectedResults.sorted))

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

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  test(
    "create ppl correlation approximate query with filters and two tables correlating on a single field test") {
    val frame = sql(s"""
         | source = $testTable1, $testTable2| correlate approximate fields(name) scope(month, 1W) mapping($testTable1.name = $testTable2.name)
         | """.stripMargin)
    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(
      Row("David", 40, "Washington", "USA", 2023, 4, "David", "Doctor", "USA", 120000, 2023, 4),
      Row("David", 40, "Washington", "USA", 2023, 4, "David", "Unemployed", "Canada", 0, 2023, 4),
      Row("Hello", 30, "New York", "USA", 2023, 4, "Hello", "Artist", "USA", 70000, 2023, 4),
      Row(
        "Jake",
        70,
        "California",
        "USA",
        2023,
        4,
        "Jake",
        "Engineer",
        "England",
        100000,
        2023,
        4),
      Row("Jane", 20, "Quebec", "Canada", 2023, 4, "Jane", "Scientist", "Canada", 90000, 2023, 4),
      Row("Jim", 27, "B.C", "Canada", 2023, 4, null, null, null, null, null, null),
      Row("John", 25, "Ontario", "Canada", 2023, 4, "John", "Doctor", "Canada", 120000, 2023, 4),
      Row("Peter", 57, "B.C", "Canada", 2023, 4, null, null, null, null, null, null),
      Row("Rick", 70, "B.C", "Canada", 2023, 4, null, null, null, null, null, null))

    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    // Compare the results
    assert(results.sorted.sameElements(expectedResults.sorted))

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

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }
  test(
    "create ppl correlation approximate query with two tables correlating on a single field and not scope test") {
    val frame = sql(s"""
         | source = $testTable1, $testTable2| correlate approximate fields(name) mapping($testTable1.name = $testTable2.name)
         | """.stripMargin)
    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(
      Row("David", 40, "Washington", "USA", 2023, 4, "David", "Doctor", "USA", 120000, 2023, 4),
      Row("David", 40, "Washington", "USA", 2023, 4, "David", "Unemployed", "Canada", 0, 2023, 4),
      Row("Hello", 30, "New York", "USA", 2023, 4, "Hello", "Artist", "USA", 70000, 2023, 4),
      Row(
        "Jake",
        70,
        "California",
        "USA",
        2023,
        4,
        "Jake",
        "Engineer",
        "England",
        100000,
        2023,
        4),
      Row("Jane", 20, "Quebec", "Canada", 2023, 4, "Jane", "Scientist", "Canada", 90000, 2023, 4),
      Row("Jim", 27, "B.C", "Canada", 2023, 4, null, null, null, null, null, null),
      Row("John", 25, "Ontario", "Canada", 2023, 4, "John", "Doctor", "Canada", 120000, 2023, 4),
      Row("Peter", 57, "B.C", "Canada", 2023, 4, null, null, null, null, null, null),
      Row("Rick", 70, "B.C", "Canada", 2023, 4, null, null, null, null, null, null))

    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    // Compare the results
    assert(results.sorted.sameElements(expectedResults.sorted))

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

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  test(
    "create ppl correlation query with with filters and two tables correlating on a two fields test") {
    val frame = sql(s"""
         | source = $testTable1, $testTable2| where year = 2023 AND month = 4 | correlate exact fields(name, country) scope(month, 1W)
         | mapping($testTable1.name = $testTable2.name, $testTable1.country = $testTable2.country)
         | """.stripMargin)
    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(
      Row("David", 40, "Washington", "USA", 2023, 4, "David", "Doctor", "USA", 120000, 2023, 4),
      Row("Hello", 30, "New York", "USA", 2023, 4, "Hello", "Artist", "USA", 70000, 2023, 4),
      Row("John", 25, "Ontario", "Canada", 2023, 4, "John", "Doctor", "Canada", 120000, 2023, 4),
      Row("Jane", 20, "Quebec", "Canada", 2023, 4, "Jane", "Scientist", "Canada", 90000, 2023, 4))

    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    // Compare the results
    assert(results.sorted.sameElements(expectedResults.sorted))

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

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  // create ppl correlation query with two tables correlating on a two fields and disjoint filters test
  ignore("skip for now due to #506 & #508") {
    val frame = sql(s"""
         | source = $testTable1, $testTable2| where year = 2023 AND month = 4 AND $testTable2.salary > 100000 | correlate exact fields(name, country) scope(month, 1W)
         | mapping($testTable1.name = $testTable2.name, $testTable1.country = $testTable2.country)
         | """.stripMargin)
    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(
      Row("David", 40, "Washington", "USA", 2023, 4, "David", "Doctor", "USA", 120000, 2023, 4),
      Row("John", 25, "Ontario", "Canada", 2023, 4, "John", "Doctor", "Canada", 120000, 2023, 4))

    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    // Compare the results
    assert(results.sorted.sameElements(expectedResults.sorted))

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

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  test(
    "create ppl correlation (exact) query with two tables correlating by name and group by avg salary by age span (10 years bucket) test") {
    val frame = sql(s"""
         | source = $testTable1, $testTable2| correlate exact fields(name) scope(month, 1W)
         | mapping($testTable1.name = $testTable2.name) |
         | stats avg(salary) by span(age, 10) as age_span
         | """.stripMargin)
    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] =
      Array(Row(100000.0, 70), Row(105000.0, 20), Row(60000.0, 40), Row(70000.0, 30))

    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Double](_.getAs[Double](0))
    // Compare the results
    assert(results.sorted.sameElements(expectedResults.sorted))

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

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  test(
    "create ppl correlation (exact) query with two tables correlating by name and group by avg salary by age span (10 years bucket) and country test") {
    val frame = sql(s"""
         | source = $testTable1, $testTable2| correlate exact fields(name) scope(month, 1W)
         | mapping($testTable1.name = $testTable2.name) |
         | stats avg(salary) by span(age, 10) as age_span, $testTable2.country
         | """.stripMargin)
    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(
      Row(120000.0, "USA", 40),
      Row(0.0, "Canada", 40),
      Row(70000.0, "USA", 30),
      Row(100000.0, "England", 70),
      Row(105000.0, "Canada", 20))

    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Double](_.getAs[Double](0))
    // Compare the results
    assert(results.sorted.sameElements(expectedResults.sorted))

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

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  test(
    "create ppl correlation (exact) query with two tables correlating by name,country and group by avg salary by age span (10 years bucket) with country filter test") {
    val frame = sql(s"""
         | source = $testTable1, $testTable2 | where country = 'USA' OR country = 'England' |
         | correlate exact fields(name) scope(month, 1W) mapping($testTable1.name = $testTable2.name) |
         | stats avg(salary) by span(age, 10) as age_span, $testTable2.country
         | """.stripMargin)
    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] =
      Array(Row(120000.0, "USA", 40), Row(100000.0, "England", 70), Row(70000.0, "USA", 30))

    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Double](_.getAs[Double](0))
    // Compare the results
    assert(results.sorted.sameElements(expectedResults.sorted))

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

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }
  test(
    "create ppl correlation (exact) query with two tables correlating by name,country and group by avg salary by age span (10 years bucket) with country filter without scope test") {
    val frame = sql(s"""
         | source = $testTable1, $testTable2 | where country = 'USA' OR country = 'England' |
         | correlate exact fields(name) mapping($testTable1.name = $testTable2.name) |
         | stats avg(salary) by span(age, 10) as age_span, $testTable2.country
         | """.stripMargin)
    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] =
      Array(Row(120000.0, "USA", 40), Row(100000.0, "England", 70), Row(70000.0, "USA", 30))

    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Double](_.getAs[Double](0))
    // Compare the results
    assert(results.sorted.sameElements(expectedResults.sorted))

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

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  test(
    "create ppl correlation (approximate) query with two tables correlating by name,country and group by avg salary by age span (10 years bucket) test") {
    val frame = sql(s"""
         | source = $testTable1, $testTable2| correlate approximate fields(name, country) scope(month, 1W)
         | mapping($testTable1.name = $testTable2.name, $testTable1.country = $testTable2.country) |
         | stats avg(salary) by span(age, 10) as age_span, $testTable2.country | sort - age_span | head 5
         | """.stripMargin)
    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(
      Row(70000.0, "Canada", 70L),
      Row(100000.0, "England", 70L),
      Row(95000.0, "USA", 70L),
      Row(70000.0, "Canada", 50L),
      Row(95000.0, "USA", 40L))

    // Define ordering for rows that first compares by age then by name
    implicit val rowOrdering: Ordering[Row] = new Ordering[Row] {
      def compare(x: Row, y: Row): Int = {
        val ageCompare = x.getAs[Long](2).compareTo(y.getAs[Long](2))
        if (ageCompare != 0) ageCompare
        else x.getAs[String](1).compareTo(y.getAs[String](1))
      }
    }

    // Compare the results
    assert(results.sorted.sameElements(expectedResults.sorted))

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

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }
}
