/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{And, Ascending, EqualTo, Literal, SortOrder}
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
         |        ('Jane', 20, 'Quebec', 'Canada')
         | """.stripMargin)
    // Insert data into the new table
    sql(s"""
         | INSERT INTO $testTable2
         | PARTITION (year=2023, month=4)
         | VALUES ('Jake', 'Engineer', 100000),
         |        ('Hello', 'Artist', 70000),
         |        ('John', 'Doctor', 120000),
         |        ('Jane', 'Scientist', 90000)
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
      Row("Jake", 70, "California", "USA", 2023, 4, "Jake", "Engineer", 100000, 2023, 4),
      Row("Hello", 30, "New York", "USA", 2023, 4, "Hello", "Artist", 70000, 2023, 4),
      Row("John", 25, "Ontario", "Canada", 2023, 4, "John", "Doctor", 120000, 2023, 4),
      Row("Jane", 20, "Quebec", "Canada", 2023, 4, "Jane", "Scientist", 90000, 2023, 4))

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

}
