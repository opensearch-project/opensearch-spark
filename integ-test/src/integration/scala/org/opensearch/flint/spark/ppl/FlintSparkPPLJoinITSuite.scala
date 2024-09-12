/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{And, EqualTo, Literal}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Join, JoinHint, LogicalPlan, Project}
import org.apache.spark.sql.streaming.StreamTest

class FlintSparkPPLJoinITSuite
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

  test("test join with filters and two tables") {
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

    val frame = sql(
      s"""
         | source = $testTable1 | inner join hint.left=a, hint.right=b
         |   ON a.year = 2023 AND a.month = 4 AND b.year = 2023 AND b.month = 4
         |   $testTable2 | fields a.name, a.age, a.state, a.country, b.occupation, b.salary
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
}
