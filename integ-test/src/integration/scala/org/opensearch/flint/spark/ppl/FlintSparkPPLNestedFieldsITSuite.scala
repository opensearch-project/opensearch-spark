/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{Ascending, Literal, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.command.DescribeTableCommand
import org.apache.spark.sql.streaming.StreamTest
import org.apache.spark.sql.{QueryTest, Row}

class FlintSparkPPLNestedFieldsITSuite
    extends QueryTest
    with LogicalPlanTestUtils
    with FlintPPLSuite
    with StreamTest {

  /** Test table and index name */
  private val testTable = "spark_catalog.default.flint_ppl_test"

  override def beforeAll(): Unit = {
    super.beforeAll()

    createStructNestedTable(testTable)
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
           describe flint_ppl_test
           """.stripMargin)

      // Retrieve the results
      val results: Array[Row] = frame.collect()
      // Define the expected results
      val expectedResults: Array[Row] = Array(
        Row("int_col", "int", null),
        Row("struct_col", "struct<field1:struct<subfield:string>,field2:int>", null))

      // Compare the results
      implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
      assert(results.sorted.sameElements(expectedResults.sorted))
      // Retrieve the logical plan
      val logicalPlan: LogicalPlan =
        frame.queryExecution.commandExecuted.asInstanceOf[CommandResult].commandLogicalPlan
      // Define the expected logical plan
      val expectedPlan: LogicalPlan =
        DescribeTableCommand(
          TableIdentifier("flint_ppl_test"),
          Map.empty[String, String],
          isExtended = false,
          output = DescribeRelation.getOutputAttrs)
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
          Row(30, Row(Row("value1"), 123)),
          Row(40, Row(Row("value5"), 123)),
          Row(30, Row(Row("value4"), 823)),
          Row(40, Row(Row("value2"), 456)),
          Row(50, Row(Row("value3"), 789)))
      // Compare the results
      implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Int](_.getAs[Int](0))
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

  test("create ppl simple query with head (limit) 1 test") {
    val frame = sql(s"""
         | source = $testTable| head 1
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    assert(results.length == 1)

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val limitPlan: LogicalPlan =
      Limit(Literal(1), UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test")))
    val expectedPlan = Project(Seq(UnresolvedStar(None)), limitPlan)

    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  test("create ppl simple query with head (limit) and sorted test") {
    val frame = sql(s"""
         | source = $testTable| sort int_col | head 1
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    assert(results.length == 1)

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical

    val sortedPlan: LogicalPlan =
      Sort(
        Seq(SortOrder(UnresolvedAttribute("int_col"), Ascending)),
        global = true,
        UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test")))

    // Define the expected logical plan
    val expectedPlan: LogicalPlan =
      Project(Seq(UnresolvedStar(None)), Limit(Literal(1), sortedPlan))

    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  test("create ppl simple query with head (limit) and nested column sorted test") {
    val frame = sql(s"""
         | source = $testTable| sort struct_col.field1 | head 1
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    assert(results.length == 1)

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical

    val sortedPlan: LogicalPlan =
      Sort(
        Seq(SortOrder(UnresolvedAttribute("struct_col.field1"), Ascending)),
        global = true,
        UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test")))

    // Define the expected logical plan
    val expectedPlan: LogicalPlan =
      Project(Seq(UnresolvedStar(None)), Limit(Literal(1), sortedPlan))

    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  test("create ppl simple query two with fields result test") {
    val frame = sql(s"""
         | source = $testTable| fields int_col, struct_col.field2
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] =
      Array(Row( 30, 123),
            Row( 30, 823),
            Row( 40, 123),
            Row( 40, 456),
           Row( 50, 789))
    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val expectedPlan: LogicalPlan = Project(
      Seq(UnresolvedAttribute("int_col"), UnresolvedAttribute("struct_col.field2")),
      UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test")))
    // Compare the two plans
    assert(expectedPlan === logicalPlan)
  }

  test("create ppl simple sorted query two with fields result test sorted") {
    val frame = sql(s"""
         | source = $testTable| sort int_col | fields int_col, struct_col.field2
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] =
      Array(Row(30, 123),
        Row(30, 823),
        Row(40, 123),
        Row(40, 456),
        Row(50, 789))
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

test("create ppl simple query with nested field range filter test") {
    val frame = sql(s"""
         | source = $testTable| where struct_col.field2 > 200 | fields  int_col, struct_col.field2
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] =
      Array(Row(30, 823),
            Row(40, 456),
            Row(50, 789))
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
  
test("create ppl simple query with nested field string filter test") {
    val frame = sql(s"""
         | source = $testTable| where struct_col.field1.subfield = `value1` | fields  int_col, struct_col.field1.subfield
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] =
      Array(Row(30, "value1"))
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
}
