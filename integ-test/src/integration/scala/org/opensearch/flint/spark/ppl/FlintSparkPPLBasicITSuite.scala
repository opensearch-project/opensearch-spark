/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedRelation, UnresolvedStar, UnresolvedTableOrView}
import org.apache.spark.sql.catalyst.expressions.{Ascending, Literal, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.command.DescribeTableCommand
import org.apache.spark.sql.streaming.StreamTest

class FlintSparkPPLBasicITSuite
    extends QueryTest
    with LogicalPlanTestUtils
    with FlintPPLSuite
    with StreamTest {

  /** Test table and index name */
  private val testTable = "spark_catalog.default.flint_ppl_test"

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Create test table
    createPartitionedStateCountryTable(testTable)
  }

  protected override def afterEach(): Unit = {
    super.afterEach()
    // Stop all streaming jobs if any
    spark.streams.active.foreach { job =>
      job.stop()
      job.awaitTermination()
    }
  }

  test("describe table query test") {
    val testTableQuoted = "`spark_catalog`.`default`.`flint_ppl_test`"
    Seq(testTable, testTableQuoted).foreach { table =>
      val frame = sql(s"""
           describe $table
           """.stripMargin)

      // Retrieve the results
      val results: Array[Row] = frame.collect()
      // Define the expected results
      val expectedResults: Array[Row] = Array(
        Row("name", "string", null),
        Row("age", "int", null),
        Row("state", "string", null),
        Row("country", "string", null),
        Row("year", "int", null),
        Row("month", "int", null),
        Row("# Partition Information", "", ""),
        Row("# col_name", "data_type", "comment"),
        Row("year", "int", null),
        Row("month", "int", null)
      )
      // Compare the results
      implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
      assert(results.sorted.sameElements(expectedResults.sorted))
      // Retrieve the logical plan
      val logicalPlan: LogicalPlan = frame.queryExecution.commandExecuted.asInstanceOf[CommandResult].commandLogicalPlan
      // Define the expected logical plan
      val expectedPlan: LogicalPlan =
          DescribeTableCommand(TableIdentifier("default.flint_ppl_test"), null, isExtended = false, output = Seq())
      // Compare the two plans
      comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
    }
  }
  
 test("create ppl simple query test") {
    val testTableQuoted = "`spark_catalog`.`default`.`flint_ppl_test`"
    Seq(testTable, testTableQuoted).foreach { table =>
      val frame = sql(s"""
           | source = $table
           | """.stripMargin)

      // Retrieve the results
      val results: Array[Row] = frame.collect()
      // Define the expected results
      val expectedResults: Array[Row] = Array(
        Row("Jake", 70, "California", "USA", 2023, 4),
        Row("Hello", 30, "New York", "USA", 2023, 4),
        Row("John", 25, "Ontario", "Canada", 2023, 4),
        Row("Jane", 20, "Quebec", "Canada", 2023, 4))
      // Compare the results
      implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
      assert(results.sorted.sameElements(expectedResults.sorted))

      // Retrieve the logical plan
      val logicalPlan: LogicalPlan = frame.queryExecution.logical
      // Define the expected logical plan
      val expectedPlan: LogicalPlan =
        Project(
          Seq(UnresolvedStar(None)),
          UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test")))
      // Compare the two plans
      assert(expectedPlan === logicalPlan)
    }
  }

  test("create ppl simple query with head (limit) 3 test") {
    val frame = sql(s"""
         | source = $testTable| head 2
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    assert(results.length == 2)

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val limitPlan: LogicalPlan =
      Limit(Literal(2), UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test")))
    val expectedPlan = Project(Seq(UnresolvedStar(None)), limitPlan)

    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  test("create ppl simple query with head (limit) and sorted test") {
    val frame = sql(s"""
         | source = $testTable| sort name | head 2
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    assert(results.length == 2)

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical

    val sortedPlan: LogicalPlan =
      Sort(
        Seq(SortOrder(UnresolvedAttribute("name"), Ascending)),
        global = true,
        UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test")))

    // Define the expected logical plan
    val expectedPlan: LogicalPlan =
      Project(Seq(UnresolvedStar(None)), Limit(Literal(2), sortedPlan))

    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  test("create ppl simple query two with fields result test") {
    val frame = sql(s"""
         | source = $testTable| fields name, age
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] =
      Array(Row("Jake", 70), Row("Hello", 30), Row("John", 25), Row("Jane", 20))
    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val expectedPlan: LogicalPlan = Project(
      Seq(UnresolvedAttribute("name"), UnresolvedAttribute("age")),
      UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test")))
    // Compare the two plans
    assert(expectedPlan === logicalPlan)
  }

  test("create ppl simple sorted query two with fields result test sorted") {
    val frame = sql(s"""
         | source = $testTable| sort age | fields name, age
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] =
      Array(Row("Jane", 20), Row("John", 25), Row("Hello", 30), Row("Jake", 70))
    assert(results === expectedResults)

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical

    val sortedPlan: LogicalPlan =
      Sort(
        Seq(SortOrder(UnresolvedAttribute("age"), Ascending)),
        global = true,
        UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test")))

    // Define the expected logical plan
    val expectedPlan: LogicalPlan =
      Project(Seq(UnresolvedAttribute("name"), UnresolvedAttribute("age")), sortedPlan)

    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  test("create ppl simple query two with fields and head (limit) test") {
    val frame = sql(s"""
         | source = $testTable| fields name, age | head 1
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    assert(results.length == 1)

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val project = Project(
      Seq(UnresolvedAttribute("name"), UnresolvedAttribute("age")),
      UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test")))
    // Define the expected logical plan
    val limitPlan: LogicalPlan = Limit(Literal(1), project)
    val expectedPlan: LogicalPlan = Project(Seq(UnresolvedStar(None)), limitPlan)
    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  test("create ppl simple query two with fields and head (limit) with sorting test") {
    Seq(("name, age", "age"), ("`name`, `age`", "`age`")).foreach {
      case (selectFields, sortField) =>
        val frame = sql(s"""
             | source = $testTable| fields $selectFields | head 1 | sort $sortField
             | """.stripMargin)

        // Retrieve the results
        val results: Array[Row] = frame.collect()
        assert(results.length == 1)

        // Retrieve the logical plan
        val logicalPlan: LogicalPlan = frame.queryExecution.logical
        val project = Project(
          Seq(UnresolvedAttribute("name"), UnresolvedAttribute("age")),
          UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test")))
        // Define the expected logical plan
        val limitPlan: LogicalPlan = Limit(Literal(1), project)
        val sortedPlan: LogicalPlan =
          Sort(Seq(SortOrder(UnresolvedAttribute("age"), Ascending)), global = true, limitPlan)

        val expectedPlan = Project(Seq(UnresolvedStar(None)), sortedPlan)
        // Compare the two plans
        assert(compareByString(expectedPlan) === compareByString(logicalPlan))
    }
  }
}
