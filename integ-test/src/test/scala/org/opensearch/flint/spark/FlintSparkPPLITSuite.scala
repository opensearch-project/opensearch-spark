/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{Alias, EqualTo, GreaterThan, LessThanOrEqual, Literal, Not}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project}
import org.apache.spark.sql.streaming.StreamTest
import org.apache.spark.sql.{QueryTest, Row}

class FlintSparkPPLITSuite
  extends QueryTest
    with LogicalPlanTestUtils
    with FlintPPLSuite
    with StreamTest {

  /** Test table and index name */
  private val testTable = "default.flint_ppl_test"

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Create test table
    sql(
      s"""
         | CREATE TABLE $testTable
         | (
         |   name STRING,
         |   age INT
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

    // Insert data
    sql(
      s"""
         | INSERT INTO $testTable
         | PARTITION (year=2023, month=4)
         | VALUES ('Jake', 70),
         |        ('Hello', 30),
         |        ('John', 25),
         |        ('Jane', 25)
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

  test("create ppl simple query with start fields result test") {
    val frame = sql(
      s"""
         | source = $testTable
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(
      Row("Jake", 70, 2023, 4),
      Row("Hello", 30, 2023, 4),
      Row("John", 25, 2023, 4),
      Row("Jane", 25, 2023, 4)
    )
    // Compare the results
    assert(results === expectedResults)

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val expectedPlan: LogicalPlan = Project(Seq(UnresolvedStar(None)), UnresolvedRelation(Seq("default", "flint_ppl_test")))
    // Compare the two plans
    assert(expectedPlan === logicalPlan)
  }

  test("create ppl simple query two with fields result test") {
    val frame = sql(
      s"""
         | source = $testTable| fields name, age
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(
      Row("Jake", 70),
      Row("Hello", 30),
      Row("John", 25),
      Row("Jane", 25)
    )
    // Compare the results
    assert(results === expectedResults)

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val expectedPlan: LogicalPlan = Project(Seq(UnresolvedAttribute("name"), UnresolvedAttribute("age")),
      UnresolvedRelation(Seq("default", "flint_ppl_test")))
    // Compare the two plans
    assert(expectedPlan === logicalPlan)
  }

  test("create ppl simple age literal equal filter query with two fields result test") {
    val frame = sql(
      s"""
         | source = $testTable age=25 | fields name, age
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(
      Row("John", 25),
      Row("Jane", 25)
    )
    // Compare the results
    assert(results === expectedResults)


    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val table = UnresolvedRelation(Seq("default", "flint_ppl_test"))
    val filterExpr = EqualTo(UnresolvedAttribute("age"), Literal(25))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedAttribute("name"), UnresolvedAttribute("age"))
    val expectedPlan = Project(projectList, filterPlan)
    // Compare the two plans
    assert(expectedPlan === logicalPlan)
  }

  test("create ppl simple age literal greater than filter query with two fields result test") {
    val frame = sql(
      s"""
         | source = $testTable age>25 | fields name, age
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(
      Row("Jake", 70),
      Row("Hello", 30)
    )
    // Compare the results
    assert(results === expectedResults)

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val table = UnresolvedRelation(Seq("default", "flint_ppl_test"))
    val filterExpr = GreaterThan(UnresolvedAttribute("age"), Literal(25))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedAttribute("name"), UnresolvedAttribute("age"))
    val expectedPlan = Project(projectList, filterPlan)
    // Compare the two plans
    assert(expectedPlan === logicalPlan)
  }

  test("create ppl simple age literal smaller than equals filter query with two fields result test") {
    val frame = sql(
      s"""
         | source = $testTable age<=65 | fields name, age
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(
      Row("Hello", 30),
      Row("John", 25),
      Row("Jane", 25)
    )
    // Compare the results
    assert(results === expectedResults)

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val table = UnresolvedRelation(Seq("default", "flint_ppl_test"))
    val filterExpr = LessThanOrEqual(UnresolvedAttribute("age"), Literal(65))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedAttribute("name"), UnresolvedAttribute("age"))
    val expectedPlan = Project(projectList, filterPlan)
    // Compare the two plans
    assert(expectedPlan === logicalPlan)
  }

  test("create ppl simple name literal equal filter query with two fields result test") {
    val frame = sql(
      s"""
         | source = $testTable name='Jake' | fields name, age
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(
      Row("Jake", 70)
    )
    //     Compare the results
    assert(results === expectedResults)

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val table = UnresolvedRelation(Seq("default", "flint_ppl_test"))
    val filterExpr = EqualTo(UnresolvedAttribute("name"), Literal("Jake"))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedAttribute("name"), UnresolvedAttribute("age"))
    val expectedPlan = Project(projectList, filterPlan)
    // Compare the two plans
    assert(expectedPlan === logicalPlan)
  }

  test("create ppl simple name literal not equal filter query with two fields result test") {
    val frame = sql(
      s"""
         | source = $testTable name!='Jake' | fields name, age
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(
      Row("Hello", 30),
      Row("John", 25),
      Row("Jane", 25)
    )

    // Compare the results
    assert(results === expectedResults)
    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val table = UnresolvedRelation(Seq("default", "flint_ppl_test"))
    val filterExpr = Not(EqualTo(UnresolvedAttribute("name"), Literal("Jake")))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedAttribute("name"), UnresolvedAttribute("age"))
    val expectedPlan = Project(projectList, filterPlan)
    // Compare the two plans
    assert(expectedPlan === logicalPlan)
  }

  test("create ppl simple age avg query test") {
    val frame = sql(
      s"""
         | source = $testTable| stats avg(age) 
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(
      Row(37.5),
    )

    // Compare the results
    assert(results === expectedResults)

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val priceField = UnresolvedAttribute("age")
    val table = UnresolvedRelation(Seq("default", "flint_ppl_test"))
    val aggregateExpressions = Seq(Alias(UnresolvedFunction(Seq("AVG"), Seq(priceField), isDistinct = false), "avg(age)")())
    val aggregatePlan = Project(aggregateExpressions, table)

    // Compare the two plans
    assert(compareByString(aggregatePlan) === compareByString(logicalPlan))
  }

  ignore("create ppl simple age avg group by query test ") {
    val checkData = sql(s"SELECT name, AVG(age) AS avg_age FROM $testTable group by name");
    checkData.show()
    checkData.queryExecution.logical.show()

    val frame = sql(
      s"""
         | source = $testTable| stats avg(age) by name
         | """.stripMargin)


    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(
      Row(37.5),
    )

    // Compare the results
    assert(results === expectedResults)

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val priceField = UnresolvedAttribute("price")
    val table = UnresolvedRelation(Seq("default", "flint_ppl_test"))
    val aggregateExpressions = Seq(Alias(UnresolvedFunction(Seq("AVG"), Seq(priceField), isDistinct = false), "avg(price)")())
    val aggregatePlan = Project( aggregateExpressions, table)

    // Compare the two plans
    assert(aggregatePlan === logicalPlan)
  }
}
