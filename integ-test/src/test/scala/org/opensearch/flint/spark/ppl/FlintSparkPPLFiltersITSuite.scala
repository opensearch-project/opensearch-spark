/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import org.opensearch.flint.spark.FlintPPLSuite

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Ascending, Descending, Divide, EqualTo, Floor, GreaterThan, LessThanOrEqual, Literal, Multiply, Not, Or, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.streaming.StreamTest

class FlintSparkPPLFiltersITSuite
    extends QueryTest
    with LogicalPlanTestUtils
    with FlintPPLSuite
    with StreamTest {

  /** Test table and index name */
  private val testTable = "default.flint_ppl_test"

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Create test table
    // Update table creation
    sql(s"""
         | CREATE TABLE $testTable
         | (
         |   name STRING,
         |   age INT,
         |   state STRING,
         |   country STRING
         | )
         | USING CSV
         | OPTIONS (
         |  header 'false',
         |  delimiter '\t'
         | )
         | PARTITIONED BY (
         |    year INT,
         |    month INT
         | )
         |""".stripMargin)

    // Update data insertion
    sql(s"""
         | INSERT INTO $testTable
         | PARTITION (year=2023, month=4)
         | VALUES ('Jake', 70, 'California', 'USA'),
         |        ('Hello', 30, 'New York', 'USA'),
         |        ('John', 25, 'Ontario', 'Canada'),
         |        ('Jane', 20, 'Quebec', 'Canada')
         | """.stripMargin)
  }

  protected override def afterEach(): Unit = {
    super.afterEach()
    // Stop all streaming jobs if any
    spark.streams.active.foreach { job =>
      job.stop()
      job.awaitTermination()
    }
  }

  test("create ppl simple age literal equal filter query with two fields result test") {
    val frame = sql(s"""
         | source = $testTable age=25 | fields name, age
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(Row("John", 25))
    // Compare the results
    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val table = UnresolvedRelation(Seq("default", "flint_ppl_test"))
    val filterExpr = EqualTo(UnresolvedAttribute("age"), Literal(25))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedAttribute("name"), UnresolvedAttribute("age"))
    val expectedPlan = Project(projectList, filterPlan)
    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  test(
    "create ppl simple age literal greater than filter AND country not equal filter query with two fields result test") {
    val frame = sql(s"""
         | source = $testTable age>10 and country != 'USA' | fields name, age
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(Row("John", 25), Row("Jane", 20))
    // Compare the results
    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val table = UnresolvedRelation(Seq("default", "flint_ppl_test"))
    val filterExpr = And(
      GreaterThan(UnresolvedAttribute("age"), Literal(10)),
      Not(EqualTo(UnresolvedAttribute("country"), Literal("USA"))))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedAttribute("name"), UnresolvedAttribute("age"))
    val expectedPlan = Project(projectList, filterPlan)
    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  test(
    "create ppl simple age literal greater than filter AND country not equal filter query with two fields sorted result test") {
    val frame = sql(s"""
         | source = $testTable age>10 and country != 'USA' | sort - age | fields name, age
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(Row("John", 25), Row("Jane", 20))
    // Compare the results
    assert(results === expectedResults)

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val table = UnresolvedRelation(Seq("default", "flint_ppl_test"))
    val filterExpr = And(
      GreaterThan(UnresolvedAttribute("age"), Literal(10)),
      Not(EqualTo(UnresolvedAttribute("country"), Literal("USA"))))
    val filterPlan = Filter(filterExpr, table)
    val sortedPlan: LogicalPlan =
      Sort(Seq(SortOrder(UnresolvedAttribute("age"), Descending)), global = true, filterPlan)
    val expectedPlan =
      Project(Seq(UnresolvedAttribute("name"), UnresolvedAttribute("age")), sortedPlan)
    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  test(
    "create ppl simple age literal equal than filter OR country not equal filter query with two fields result test") {
    val frame = sql(s"""
         | source = $testTable age<=20 OR country = 'USA' | fields name, age
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(Row("Jane", 20), Row("Jake", 70), Row("Hello", 30))
    // Compare the results
    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val table = UnresolvedRelation(Seq("default", "flint_ppl_test"))
    val filterExpr = Or(
      LessThanOrEqual(UnresolvedAttribute("age"), Literal(20)),
      EqualTo(UnresolvedAttribute("country"), Literal("USA")))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedAttribute("name"), UnresolvedAttribute("age"))
    val expectedPlan = Project(projectList, filterPlan)
    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  test(
    "create ppl simple age literal equal than filter OR country not equal filter query with two fields result and head (limit) test") {
    val frame = sql(s"""
         | source = $testTable age<=20 OR country = 'USA' | fields name, age | head 1
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    assert(results.length == 1)

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val table = UnresolvedRelation(Seq("default", "flint_ppl_test"))
    val filterExpr = Or(
      LessThanOrEqual(UnresolvedAttribute("age"), Literal(20)),
      EqualTo(UnresolvedAttribute("country"), Literal("USA")))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedAttribute("name"), UnresolvedAttribute("age"))
    val limitPlan = Limit(Literal(1), Project(projectList, filterPlan))
    val expectedPlan = Project(Seq(UnresolvedStar(None)), limitPlan)
    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  test("create ppl simple age literal greater than filter query with two fields result test") {
    val frame = sql(s"""
         | source = $testTable age>25 | fields name, age
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(Row("Jake", 70), Row("Hello", 30))
    // Compare the results
    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val table = UnresolvedRelation(Seq("default", "flint_ppl_test"))
    val filterExpr = GreaterThan(UnresolvedAttribute("age"), Literal(25))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedAttribute("name"), UnresolvedAttribute("age"))
    val expectedPlan = Project(projectList, filterPlan)
    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  test(
    "create ppl simple age literal smaller than equals filter query with two fields result test") {
    val frame = sql(s"""
         | source = $testTable age<=65 | fields name, age
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(Row("Hello", 30), Row("John", 25), Row("Jane", 20))
    // Compare the results
    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val table = UnresolvedRelation(Seq("default", "flint_ppl_test"))
    val filterExpr = LessThanOrEqual(UnresolvedAttribute("age"), Literal(65))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedAttribute("name"), UnresolvedAttribute("age"))
    val expectedPlan = Project(projectList, filterPlan)
    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  test(
    "create ppl simple age literal smaller than equals filter query with two fields result with sort test") {
    val frame = sql(s"""
         | source = $testTable age<=65 | sort name | fields name, age
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(Row("Hello", 30), Row("Jane", 20), Row("John", 25))
    // Compare the results
    assert(results === expectedResults)

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val table = UnresolvedRelation(Seq("default", "flint_ppl_test"))
    val filterExpr = LessThanOrEqual(UnresolvedAttribute("age"), Literal(65))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedAttribute("name"), UnresolvedAttribute("age"))
    val sortedPlan: LogicalPlan =
      Sort(Seq(SortOrder(UnresolvedAttribute("name"), Ascending)), global = true, filterPlan)
    val expectedPlan = Project(projectList, sortedPlan)
    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  test("create ppl simple name literal equal filter query with two fields result test") {
    val frame = sql(s"""
         | source = $testTable name='Jake' | fields name, age
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(Row("Jake", 70))
    //     Compare the results
    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Double](_.getAs[Double](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val table = UnresolvedRelation(Seq("default", "flint_ppl_test"))
    val filterExpr = EqualTo(UnresolvedAttribute("name"), Literal("Jake"))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedAttribute("name"), UnresolvedAttribute("age"))
    val expectedPlan = Project(projectList, filterPlan)
    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  test("create ppl simple name literal not equal filter query with two fields result test") {
    val frame = sql(s"""
         | source = $testTable name!='Jake' | fields name, age
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(Row("Hello", 30), Row("John", 25), Row("Jane", 20))

    // Compare the results
    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val table = UnresolvedRelation(Seq("default", "flint_ppl_test"))
    val filterExpr = Not(EqualTo(UnresolvedAttribute("name"), Literal("Jake")))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedAttribute("name"), UnresolvedAttribute("age"))
    val expectedPlan = Project(projectList, filterPlan)
    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  test("create ppl simple avg age by span of interval of 10 years query test ") {
    val frame = sql(s"""
         | source = $testTable| stats avg(age) by span(age, 10) as age_span
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(Row(70d, 70L), Row(30d, 30L), Row(22.5d, 20L))

    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Double](_.getAs[Double](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val star = Seq(UnresolvedStar(None))
    val ageField = UnresolvedAttribute("age")
    val table = UnresolvedRelation(Seq("default", "flint_ppl_test"))

    val aggregateExpressions =
      Alias(UnresolvedFunction(Seq("AVG"), Seq(ageField), isDistinct = false), "avg(age)")()
    val span = Alias(
      Multiply(Floor(Divide(UnresolvedAttribute("age"), Literal(10))), Literal(10)),
      "age_span")()
    val aggregatePlan = Aggregate(Seq(span), Seq(aggregateExpressions, span), table)
    val expectedPlan = Project(star, aggregatePlan)

    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  test(
    "create ppl simple avg age by span of interval of 10 years with head (limit) query test ") {
    val frame = sql(s"""
         | source = $testTable| stats avg(age) by span(age, 10) as age_span | head 2
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    assert(results.length == 2)

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val star = Seq(UnresolvedStar(None))
    val ageField = UnresolvedAttribute("age")
    val table = UnresolvedRelation(Seq("default", "flint_ppl_test"))

    val aggregateExpressions =
      Alias(UnresolvedFunction(Seq("AVG"), Seq(ageField), isDistinct = false), "avg(age)")()
    val span = Alias(
      Multiply(Floor(Divide(UnresolvedAttribute("age"), Literal(10))), Literal(10)),
      "age_span")()
    val aggregatePlan = Aggregate(Seq(span), Seq(aggregateExpressions, span), table)
    val limitPlan = Limit(Literal(2), aggregatePlan)
    val expectedPlan = Project(star, limitPlan)

    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  /**
   * | age_span | country | average_age |
   * |:---------|:--------|:------------|
   * | 20       | Canada  | 22.5        |
   * | 30       | USA     | 30          |
   * | 70       | USA     | 70          |
   */
  test("create ppl average age by span of interval of 10 years group by country query test ") {
    val dataFrame = spark.sql(
      "SELECT FLOOR(age / 10) * 10 AS age_span, country, AVG(age) AS average_age FROM default.flint_ppl_test GROUP BY FLOOR(age / 10) * 10, country ")
    dataFrame.collect();
    dataFrame.show()

    val frame = sql(s"""
         | source = $testTable| stats avg(age) by span(age, 10) as age_span, country
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] =
      Array(Row(70.0d, "USA", 70L), Row(30.0d, "USA", 30L), Row(22.5d, "Canada", 20L))

    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](1))
    assert(results.sorted.sameElements(expectedResults.sorted))

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val star = Seq(UnresolvedStar(None))
    val ageField = UnresolvedAttribute("age")
    val table = UnresolvedRelation(Seq("default", "flint_ppl_test"))
    val countryField = UnresolvedAttribute("country")
    val countryAlias = Alias(countryField, "country")()

    val aggregateExpressions =
      Alias(UnresolvedFunction(Seq("AVG"), Seq(ageField), isDistinct = false), "avg(age)")()
    val span = Alias(
      Multiply(Floor(Divide(UnresolvedAttribute("age"), Literal(10))), Literal(10)),
      "age_span")()
    val aggregatePlan =
      Aggregate(Seq(countryAlias, span), Seq(aggregateExpressions, countryAlias, span), table)
    val expectedPlan = Project(star, aggregatePlan)

    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

}
