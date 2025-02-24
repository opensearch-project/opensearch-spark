/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import org.opensearch.sql.ppl.utils.DataTypeTransformer.seq

import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, DataFrameDropColumns, LogicalPlan, Project}
import org.apache.spark.sql.streaming.StreamTest

class FlintSparkPPLRenameITSuite
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

  test("test renamed should remove source field") {
    val frame = sql(s"""
                       | source = $testTable | rename age as renamed_age
                       | """.stripMargin)

    // Retrieve the results
    frame.collect()
    assert(frame.columns.contains("renamed_age"))
    assert(frame.columns.length == 6)
    val expectedColumns =
      Array[String]("name", "state", "country", "year", "month", "renamed_age")
    assert(frame.columns.sameElements(expectedColumns))
  }

  test("test renamed should report error when field is referenced by its original name") {
    val ex = intercept[AnalysisException](sql(s"""
                                                 | source = $testTable | rename age as renamed_age | fields age
                                                 | """.stripMargin))
    assert(
      ex.getMessage()
        .contains(" A column or function parameter with name `age` cannot be resolved"))
  }

  test("test single renamed field in fields command") {
    val frame = sql(s"""
         | source = $testTable | rename age as renamed_age | fields name, renamed_age
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
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val fieldsProjectList = Seq(UnresolvedAttribute("name"), UnresolvedAttribute("renamed_age"))
    val renameProjectList =
      Seq(UnresolvedStar(None), Alias(UnresolvedAttribute("age"), "renamed_age")())
    val innerProject = Project(renameProjectList, table)
    val planDropColumn = DataFrameDropColumns(Seq(UnresolvedAttribute("age")), innerProject)
    val expectedPlan = Project(fieldsProjectList, planDropColumn)
    // Compare the two plans
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test multiple renamed fields in fields command") {
    val frame = sql(s"""
         | source = $testTable | rename name as renamed_name, country as renamed_country | fields renamed_name, age, renamed_country
         | """.stripMargin)

    val results: Array[Row] = frame.collect()
    // results.foreach(println(_))
    val expectedResults: Array[Row] =
      Array(
        Row("Jake", 70, "USA"),
        Row("Hello", 30, "USA"),
        Row("John", 25, "Canada"),
        Row("Jane", 20, "Canada"))
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val fieldsProjectList = Seq(
      UnresolvedAttribute("renamed_name"),
      UnresolvedAttribute("age"),
      UnresolvedAttribute("renamed_country"))
    val renameProjectList =
      Seq(
        UnresolvedStar(None),
        Alias(UnresolvedAttribute("name"), "renamed_name")(),
        Alias(UnresolvedAttribute("country"), "renamed_country")())
    val innerProject = Project(renameProjectList, table)
    val planDropColumn = DataFrameDropColumns(
      Seq(UnresolvedAttribute("name"), UnresolvedAttribute("country")),
      innerProject)
    val expectedPlan = Project(fieldsProjectList, planDropColumn)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test renamed fields without fields command") {
    val frame = sql(s"""
         | source = $testTable | rename age as user_age, country as user_country
         | """.stripMargin)

    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] = Array(
      Row("Jake", "California", 2023, 4, 70, "USA"),
      Row("Hello", "New York", 2023, 4, 30, "USA"),
      Row("John", "Ontario", 2023, 4, 25, "Canada"),
      Row("Jane", "Quebec", 2023, 4, 20, "Canada"))
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    val sorted = results.sorted
    val expectedSorted = expectedResults.sorted
    assert(sorted.sameElements(expectedSorted))

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val renameProjectList = Seq(
      UnresolvedStar(None),
      Alias(UnresolvedAttribute("age"), "user_age")(),
      Alias(UnresolvedAttribute("country"), "user_country")())
    val innerProject = Project(renameProjectList, table)
    val planDropColumn = DataFrameDropColumns(
      Seq(UnresolvedAttribute("age"), UnresolvedAttribute("country")),
      innerProject)
    val expectedPlan = Project(seq(UnresolvedStar(None)), planDropColumn)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test renamed field used in aggregation") {
    val frame = sql(s"""
         | source = $testTable | rename age as user_age | stats avg(user_age) by country
         | """.stripMargin)

    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] = Array(Row(22.5, "Canada"), Row(50.0, "USA"))
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Double](_.getAs[Double](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val renameProjectList =
      Seq(UnresolvedStar(None), Alias(UnresolvedAttribute("age"), "user_age")())
    val aggregateExpressions =
      Seq(
        Alias(
          UnresolvedFunction(
            Seq("AVG"),
            Seq(UnresolvedAttribute("user_age")),
            isDistinct = false),
          "avg(user_age)")(),
        Alias(UnresolvedAttribute("country"), "country")())
    val innerProject = Project(renameProjectList, table)
    val planDropColumn = DataFrameDropColumns(Seq(UnresolvedAttribute("age")), innerProject)
    val aggregatePlan = Aggregate(
      Seq(Alias(UnresolvedAttribute("country"), "country")()),
      aggregateExpressions,
      planDropColumn)
    val expectedPlan = Project(seq(UnresolvedStar(None)), aggregatePlan)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test multiple renamed fields with backticks alias") {
    val frame = sql(s"""
         | source = $testTable | rename name as `renamed_name`, country as `renamed_country` | fields `renamed_name`, `age`, `renamed_country`
         | """.stripMargin)

    val expectedResults: Array[Row] =
      Array(
        Row("Jake", 70, "USA"),
        Row("Hello", 30, "USA"),
        Row("John", 25, "Canada"),
        Row("Jane", 20, "Canada"))
    assertSameRows(expectedResults, frame)

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val fieldsProjectList = Seq(
      UnresolvedAttribute("renamed_name"),
      UnresolvedAttribute("age"),
      UnresolvedAttribute("renamed_country"))
    val renameProjectList =
      Seq(
        UnresolvedStar(None),
        Alias(UnresolvedAttribute("name"), "renamed_name")(),
        Alias(UnresolvedAttribute("country"), "renamed_country")())
    val innerProject = Project(renameProjectList, table)
    val planDropColumn = DataFrameDropColumns(
      Seq(UnresolvedAttribute("name"), UnresolvedAttribute("country")),
      innerProject)
    val expectedPlan = Project(fieldsProjectList, planDropColumn)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test renamed field with backticks alias used in aggregation") {
    val frame = sql(s"""
         | source = $testTable | rename age as `user_age` | stats avg(`user_age`) by country
         | """.stripMargin)

    val expectedResults: Array[Row] = Array(Row(22.5, "Canada"), Row(50.0, "USA"))
    assertSameRows(expectedResults, frame)

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val renameProjectList =
      Seq(UnresolvedStar(None), Alias(UnresolvedAttribute("age"), "user_age")())
    val aggregateExpressions =
      Seq(
        Alias(
          UnresolvedFunction(
            Seq("AVG"),
            Seq(UnresolvedAttribute("user_age")),
            isDistinct = false),
          "avg(`user_age`)")(),
        Alias(UnresolvedAttribute("country"), "country")())
    val innerProject = Project(renameProjectList, table)
    val planDropColumn = DataFrameDropColumns(Seq(UnresolvedAttribute("age")), innerProject)
    val aggregatePlan = Aggregate(
      Seq(Alias(UnresolvedAttribute("country"), "country")()),
      aggregateExpressions,
      planDropColumn)
    val expectedPlan = Project(seq(UnresolvedStar(None)), aggregatePlan)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }
}
