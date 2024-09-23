/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{Alias, Ascending, Descending, EqualTo, Literal, Not, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.ExplainMode
import org.apache.spark.sql.execution.command.{DescribeTableCommand, ExplainCommand}
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

  test("explain test") {
    val frame = sql(s"""
                       | explain | source = $testTable | where state != 'California' | fields name
                       | """.stripMargin)

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val relation = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val filter = Filter(Not(EqualTo(UnresolvedAttribute("state"), Literal("California"))), relation)
    val expectedPlan: LogicalPlan =
      ExplainCommand(
        Project(
          Seq(UnresolvedAttribute("name")),
          filter
        ),
        ExplainMode.fromString("simple")
      )
    // Compare the two plans
    assert(expectedPlan === logicalPlan)
  }

  test("describe (extended) table query test") {
    val frame = sql(s"""
           describe flint_ppl_test
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
      Row("month", "int", null))

    // Convert actual results to a Set for quick lookup
    val resultsSet: Set[Row] = results.toSet
    // Check that each expected row is present in the actual results
    expectedResults.foreach { expectedRow =>
      assert(resultsSet.contains(expectedRow), s"Expected row $expectedRow not found in results")
    }
    // Retrieve the logical plan
    val logicalPlan: LogicalPlan =
      frame.queryExecution.commandExecuted.asInstanceOf[CommandResult].commandLogicalPlan
    // Define the expected logical plan
    val expectedPlan: LogicalPlan =
      DescribeTableCommand(
        TableIdentifier("flint_ppl_test"),
        Map.empty[String, String],
        isExtended = true,
        output = DescribeRelation.getOutputAttrs)
    // Compare the two plans
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("describe (extended) FQN (2 parts) table query test") {
    val frame = sql(s"""
           describe default.flint_ppl_test
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
      Row("month", "int", null))

    // Convert actual results to a Set for quick lookup
    val resultsSet: Set[Row] = results.toSet
    // Check that each expected row is present in the actual results
    expectedResults.foreach { expectedRow =>
      assert(resultsSet.contains(expectedRow), s"Expected row $expectedRow not found in results")
    }
    // Retrieve the logical plan
    val logicalPlan: LogicalPlan =
      frame.queryExecution.commandExecuted.asInstanceOf[CommandResult].commandLogicalPlan
    // Define the expected logical plan
    val expectedPlan: LogicalPlan =
      DescribeTableCommand(
        TableIdentifier("flint_ppl_test", Option("default")),
        Map.empty[String, String],
        isExtended = true,
        output = DescribeRelation.getOutputAttrs)
    // Compare the two plans
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
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

  test("fields plus command") {
    Seq(("name, age", "age"), ("`name`, `age`", "`age`")).foreach {
      case (selectFields, sortField) =>
        val frame = sql(s"""
             | source = $testTable| fields + $selectFields | head 1 | sort $sortField
             | """.stripMargin)
        frame.show()
        val results: Array[Row] = frame.collect()
        assert(results.length == 1)

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
        comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
    }
  }

  test("fields minus command") {
    Seq(("state, country", "age"), ("`state`, `country`", "`age`")).foreach {
      case (selectFields, sortField) =>
        val frame = sql(s"""
             | source = $testTable| fields - $selectFields | sort - $sortField | head 1
             | """.stripMargin)

        val results: Array[Row] = frame.collect()
        assert(results.length == 1)
        val expectedResults: Array[Row] = Array(Row("Jake", 70, 2023, 4))
        assert(results.sameElements(expectedResults))

        val logicalPlan: LogicalPlan = frame.queryExecution.logical
        val drop = DataFrameDropColumns(
          Seq(UnresolvedAttribute("state"), UnresolvedAttribute("country")),
          UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test")))
        val sortedPlan: LogicalPlan =
          Sort(Seq(SortOrder(UnresolvedAttribute("age"), Descending)), global = true, drop)
        val limitPlan: LogicalPlan = Limit(Literal(1), sortedPlan)

        val expectedPlan = Project(Seq(UnresolvedStar(None)), limitPlan)
        // Compare the two plans
        comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
    }
  }

  test("fields minus new field added by eval") {
    val frame = sql(s"""
         | source = $testTable| eval national = country, newAge = age
         | | fields - state, national, newAge | sort - age | head 1
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    assert(results.length == 1)
    val expectedResults: Array[Row] = Array(Row("Jake", 70, "USA", 2023, 4))
    assert(results.sameElements(expectedResults))

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val evalProject = Project(
      Seq(
        UnresolvedStar(None),
        Alias(UnresolvedAttribute("country"), "national")(),
        Alias(UnresolvedAttribute("age"), "newAge")()),
      table)
    val drop = DataFrameDropColumns(
      Seq(
        UnresolvedAttribute("state"),
        UnresolvedAttribute("national"),
        UnresolvedAttribute("newAge")),
      evalProject)
    val sortedPlan: LogicalPlan =
      Sort(Seq(SortOrder(UnresolvedAttribute("age"), Descending)), global = true, drop)
    val limitPlan: LogicalPlan = Limit(Literal(1), sortedPlan)

    val expectedPlan = Project(Seq(UnresolvedStar(None)), limitPlan)
    // Compare the two plans
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  // TODO this test should work when the bug https://issues.apache.org/jira/browse/SPARK-49782 fixed.
  ignore("fields minus new function expression added by eval") {
    val frame = sql(s"""
         | source = $testTable| eval national = lower(country), newAge = age + 1
         | | fields - state, national, newAge | sort - age | head 1
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    assert(results.length == 1)
    val expectedResults: Array[Row] = Array(Row("Jake", 70, "USA", 2023, 4))
    assert(results.sameElements(expectedResults))

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val lowerFunction =
      UnresolvedFunction("lower", Seq(UnresolvedAttribute("country")), isDistinct = false)
    val addFunction =
      UnresolvedFunction("+", Seq(UnresolvedAttribute("age"), Literal(1)), isDistinct = false)
    val evalProject = Project(
      Seq(
        UnresolvedStar(None),
        Alias(lowerFunction, "national")(),
        Alias(addFunction, "newAge")()),
      table)
    val drop = DataFrameDropColumns(
      Seq(
        UnresolvedAttribute("state"),
        UnresolvedAttribute("national"),
        UnresolvedAttribute("newAge")),
      evalProject)
    val sortedPlan: LogicalPlan =
      Sort(Seq(SortOrder(UnresolvedAttribute("age"), Descending)), global = true, drop)
    val limitPlan: LogicalPlan = Limit(Literal(1), sortedPlan)

    val expectedPlan = Project(Seq(UnresolvedStar(None)), limitPlan)
    // Compare the two plans
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }
}
