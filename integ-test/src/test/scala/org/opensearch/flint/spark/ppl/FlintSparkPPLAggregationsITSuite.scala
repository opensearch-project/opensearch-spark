/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import org.opensearch.flint.spark.FlintPPLSuite

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{Alias, Ascending, EqualTo, LessThan, Literal, Not, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.streaming.StreamTest

class FlintSparkPPLAggregationsITSuite
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

  test("create ppl simple age avg query test") {
    val frame = sql(s"""
         | source = $testTable| stats avg(age)
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(Row(36.25))

    // Compare the results
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
      Seq(Alias(UnresolvedFunction(Seq("AVG"), Seq(ageField), isDistinct = false), "avg(age)")())
    val aggregatePlan = Aggregate(Seq(), aggregateExpressions, table)
    val expectedPlan = Project(star, aggregatePlan)

    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  test("create ppl simple age avg query with filter test") {
    val frame = sql(s"""
         | source = $testTable| where age < 50 | stats avg(age)
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(Row(25))

    // Compare the results
    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Double](_.getAs[Double](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val star = Seq(UnresolvedStar(None))
    val ageField = UnresolvedAttribute("age")
    val table = UnresolvedRelation(Seq("default", "flint_ppl_test"))
    val filterExpr = LessThan(ageField, Literal(50))
    val filterPlan = Filter(filterExpr, table)
    val aggregateExpressions =
      Seq(Alias(UnresolvedFunction(Seq("AVG"), Seq(ageField), isDistinct = false), "avg(age)")())
    val aggregatePlan = Aggregate(Seq(), aggregateExpressions, filterPlan)
    val expectedPlan = Project(star, aggregatePlan)

    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  test("create ppl simple age avg group by country query test ") {
    val frame = sql(s"""
         | source = $testTable| stats avg(age) by country
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(Row(22.5, "Canada"), Row(50.0, "USA"))

    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Double](_.getAs[Double](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val star = Seq(UnresolvedStar(None))
    val countryField = UnresolvedAttribute("country")
    val ageField = UnresolvedAttribute("age")
    val table = UnresolvedRelation(Seq("default", "flint_ppl_test"))

    val groupByAttributes = Seq(Alias(countryField, "country")())
    val aggregateExpressions =
      Alias(UnresolvedFunction(Seq("AVG"), Seq(ageField), isDistinct = false), "avg(age)")()
    val countryAlias = Alias(countryField, "country")()

    val aggregatePlan =
      Aggregate(groupByAttributes, Seq(aggregateExpressions, countryAlias), table)
    val expectedPlan = Project(star, aggregatePlan)

    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  test("create ppl simple age avg group by country head (limit) query test ") {
    val frame = sql(s"""
         | source = $testTable| stats avg(age) by country | head 1
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    assert(results.length == 1)

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val countryField = UnresolvedAttribute("country")
    val ageField = UnresolvedAttribute("age")
    val table = UnresolvedRelation(Seq("default", "flint_ppl_test"))

    val groupByAttributes = Seq(Alias(countryField, "country")())
    val aggregateExpressions =
      Alias(UnresolvedFunction(Seq("AVG"), Seq(ageField), isDistinct = false), "avg(age)")()
    val productAlias = Alias(countryField, "country")()

    val aggregatePlan =
      Aggregate(groupByAttributes, Seq(aggregateExpressions, productAlias), table)
    val projectPlan = Limit(Literal(1), aggregatePlan)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), projectPlan)

    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  test("create ppl simple age max group by country query test ") {
    val frame = sql(s"""
         | source = $testTable| stats max(age) by country
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(Row(70, "USA"), Row(25, "Canada"))

    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Int](_.getAs[Int](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val star = Seq(UnresolvedStar(None))
    val countryField = UnresolvedAttribute("country")
    val ageField = UnresolvedAttribute("age")
    val table = UnresolvedRelation(Seq("default", "flint_ppl_test"))

    val groupByAttributes = Seq(Alias(countryField, "country")())
    val aggregateExpressions =
      Alias(UnresolvedFunction(Seq("MAX"), Seq(ageField), isDistinct = false), "max(age)")()
    val productAlias = Alias(countryField, "country")()

    val aggregatePlan =
      Aggregate(groupByAttributes, Seq(aggregateExpressions, productAlias), table)
    val expectedPlan = Project(star, aggregatePlan)

    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  test("create ppl simple age min group by country query test ") {
    val frame = sql(s"""
         | source = $testTable| stats min(age) by country
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(Row(30, "USA"), Row(20, "Canada"))

    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Int](_.getAs[Int](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val star = Seq(UnresolvedStar(None))
    val countryField = UnresolvedAttribute("country")
    val ageField = UnresolvedAttribute("age")
    val table = UnresolvedRelation(Seq("default", "flint_ppl_test"))

    val groupByAttributes = Seq(Alias(countryField, "country")())
    val aggregateExpressions =
      Alias(UnresolvedFunction(Seq("MIN"), Seq(ageField), isDistinct = false), "min(age)")()
    val productAlias = Alias(countryField, "country")()

    val aggregatePlan =
      Aggregate(groupByAttributes, Seq(aggregateExpressions, productAlias), table)
    val expectedPlan = Project(star, aggregatePlan)

    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  test("create ppl simple age sum group by country query test ") {
    val frame = sql(s"""
         | source = $testTable| stats sum(age) by country
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(Row(100L, "USA"), Row(45L, "Canada"))

    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Long](_.getAs[Long](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val star = Seq(UnresolvedStar(None))
    val countryField = UnresolvedAttribute("country")
    val ageField = UnresolvedAttribute("age")
    val table = UnresolvedRelation(Seq("default", "flint_ppl_test"))

    val groupByAttributes = Seq(Alias(countryField, "country")())
    val aggregateExpressions =
      Alias(UnresolvedFunction(Seq("SUM"), Seq(ageField), isDistinct = false), "sum(age)")()
    val productAlias = Alias(countryField, "country")()

    val aggregatePlan =
      Aggregate(groupByAttributes, Seq(aggregateExpressions, productAlias), table)
    val expectedPlan = Project(star, aggregatePlan)

    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  test("create ppl simple age sum group by country order by age query test with sort ") {
    val frame = sql(s"""
         | source = $testTable| stats sum(age) by country | sort country
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(Row(45L, "Canada"), Row(100L, "USA"))

    // Compare the results
    assert(results === expectedResults)

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val star = Seq(UnresolvedStar(None))
    val countryField = UnresolvedAttribute("country")
    val ageField = UnresolvedAttribute("age")
    val table = UnresolvedRelation(Seq("default", "flint_ppl_test"))

    val groupByAttributes = Seq(Alias(countryField, "country")())
    val aggregateExpressions =
      Alias(UnresolvedFunction(Seq("SUM"), Seq(ageField), isDistinct = false), "sum(age)")()
    val productAlias = Alias(countryField, "country")()

    val aggregatePlan =
      Aggregate(groupByAttributes, Seq(aggregateExpressions, productAlias), table)
    val sortedPlan: LogicalPlan =
      Sort(
        Seq(SortOrder(UnresolvedAttribute("country"), Ascending)),
        global = true,
        aggregatePlan)
    val expectedPlan = Project(star, sortedPlan)
    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  test("create ppl simple age count group by country query test ") {
    val frame = sql(s"""
         | source = $testTable| stats count(age) by country
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(Row(2L, "Canada"), Row(2L, "USA"))

    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](1))
    assert(
      results.sorted.sameElements(expectedResults.sorted),
      s"Expected: ${expectedResults.mkString(", ")}, but got: ${results.mkString(", ")}")

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val star = Seq(UnresolvedStar(None))
    val countryField = UnresolvedAttribute("country")
    val ageField = UnresolvedAttribute("age")
    val table = UnresolvedRelation(Seq("default", "flint_ppl_test"))

    val groupByAttributes = Seq(Alias(countryField, "country")())
    val aggregateExpressions =
      Alias(UnresolvedFunction(Seq("COUNT"), Seq(ageField), isDistinct = false), "count(age)")()
    val productAlias = Alias(countryField, "country")()

    val aggregatePlan =
      Aggregate(groupByAttributes, Seq(aggregateExpressions, productAlias), table)
    val expectedPlan = Project(star, aggregatePlan)

    // Compare the two plans
    assert(
      compareByString(expectedPlan) === compareByString(logicalPlan),
      s"Expected plan: ${compareByString(expectedPlan)}, but got: ${compareByString(logicalPlan)}")
  }

  test("create ppl simple age avg group by country with state filter query test ") {
    val frame = sql(s"""
         | source = $testTable|  where state != 'Quebec' | stats avg(age) by country
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(Row(25.0, "Canada"), Row(50.0, "USA"))

    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Double](_.getAs[Double](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val star = Seq(UnresolvedStar(None))
    val stateField = UnresolvedAttribute("state")
    val countryField = UnresolvedAttribute("country")
    val ageField = UnresolvedAttribute("age")
    val table = UnresolvedRelation(Seq("default", "flint_ppl_test"))

    val groupByAttributes = Seq(Alias(countryField, "country")())
    val aggregateExpressions =
      Alias(UnresolvedFunction(Seq("AVG"), Seq(ageField), isDistinct = false), "avg(age)")()
    val productAlias = Alias(countryField, "country")()
    val filterExpr = Not(EqualTo(stateField, Literal("Quebec")))
    val filterPlan = Filter(filterExpr, table)

    val aggregatePlan =
      Aggregate(groupByAttributes, Seq(aggregateExpressions, productAlias), filterPlan)
    val expectedPlan = Project(star, aggregatePlan)

    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }
}
