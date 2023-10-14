/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{And, Ascending, EqualTo, GreaterThan, Literal, SortOrder}
import org.apache.spark.sql.catalyst.plans.Inner
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
    sql(s"""
         | CREATE TABLE $testTable1
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

    sql(s"""
         | CREATE TABLE $testTable2
         | (
         |   name STRING,
         |   occupation STRING,
         |   country STRING,
         |   salary INT
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
         | INSERT INTO $testTable1
         | PARTITION (year=2023, month=4)
         | VALUES ('Jake', 70, 'California', 'USA'),
         |        ('Hello', 30, 'New York', 'USA'),
         |        ('John', 25, 'Ontario', 'Canada'),
         |        ('Jim', 27,  'B.C', 'Canada'),
         |        ('Peter', 57,  'B.C', 'Canada'),
         |        ('Rick', 70,  'B.C', 'Canada'),
         |        ('David', 40,  'Washington', 'USA'),
         |        ('Jane', 20, 'Quebec', 'Canada')
         | """.stripMargin)
    // Insert data into the new table
    sql(s"""
         | INSERT INTO $testTable2
         | PARTITION (year=2023, month=4)
         | VALUES ('Jake', 'Engineer', 'England' , 100000),
         |        ('Hello', 'Artist', 'USA', 70000),
         |        ('John', 'Doctor', 'Canada', 120000),
         |        ('David', 'Doctor', 'USA', 120000),
         |        ('David', 'Unemployed', 'Canada', 0),
         |        ('Jane', 'Scientist', 'Canada', 90000)
         | """.stripMargin)
    sql(s"""
           | CREATE TABLE $testTable3
           | (
           |   name STRING,
           |   country STRING,
           |   hobby STRING,
           |   language STRING
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

    // Insert data into the new table
    sql(s"""
           | INSERT INTO $testTable3
           | PARTITION (year=2023, month=4)
           | VALUES ('Jake', 'USA', 'Fishing', 'English'),
           |        ('Hello', 'USA', 'Painting', 'English'),
           |        ('John', 'Canada', 'Reading', 'French'),
           |        ('Jim', 'Canada', 'Hiking', 'English'),
           |        ('Peter', 'Canada', 'Gaming', 'English'),
           |        ('Rick', 'USA', 'Swimming', 'English'),
           |        ('David', 'USA', 'Gardening', 'English'),
           |        ('Jane', 'Canada', 'Singing', 'French')
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

  test("create ppl correlation query with two tables correlating on a single field test") {
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

  test("create ppl correlation query with two tables correlating on a two fields test") {
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

  test(
    "create ppl correlation query with two tables correlating on a two fields and disjoint filters test") {
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
}
