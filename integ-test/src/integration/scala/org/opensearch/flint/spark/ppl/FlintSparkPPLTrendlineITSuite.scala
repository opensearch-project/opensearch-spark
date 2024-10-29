/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{Alias, Ascending, CaseWhen, CurrentRow, Descending, LessThan, Literal, RowFrame, SortOrder, SpecifiedWindowFrame, WindowExpression, WindowSpecDefinition}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.streaming.StreamTest

class FlintSparkPPLTrendlineITSuite
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

  test("test trendline sma command without fields command") {
    val frame = sql(s"""
                       | source = $testTable | sort - age | trendline sma(2, age) as age_sma
                       | """.stripMargin)

    assert(
      frame.columns.sameElements(
        Array("name", "age", "state", "country", "year", "month", "age_sma")))
    // Retrieve the results
    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] =
      Array(
        Row("Jake", 70, "California", "USA", 2023, 4, null),
        Row("Hello", 30, "New York", "USA", 2023, 4, 50.0),
        Row("John", 25, "Ontario", "Canada", 2023, 4, 27.5),
        Row("Jane", 20, "Quebec", "Canada", 2023, 4, 22.5))
    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val ageField = UnresolvedAttribute("age")
    val sort = Sort(Seq(SortOrder(ageField, Descending)), global = true, table)
    val countWindow = new WindowExpression(
      UnresolvedFunction("COUNT", Seq(Literal(1)), isDistinct = false),
      WindowSpecDefinition(Seq(), Seq(), SpecifiedWindowFrame(RowFrame, Literal(-1), CurrentRow))
    )
    val smaWindow = WindowExpression(
      UnresolvedFunction("AVG", Seq(ageField), isDistinct = false),
      WindowSpecDefinition(Seq(), Seq(), SpecifiedWindowFrame(RowFrame, Literal(-1), CurrentRow)))
    val caseWhen = CaseWhen(Seq((LessThan(countWindow, Literal(2)), Literal(null))), smaWindow)
    val trendlineProjectList = Seq(UnresolvedStar(None), Alias(caseWhen, "age_sma")())
    val expectedPlan = Project(Seq(UnresolvedStar(None)), Project(trendlineProjectList, sort))
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test trendline sma command with fields command") {
    val frame = sql(s"""
                       | source = $testTable | trendline sort - age sma(3, age) as age_sma | fields name, age, age_sma
                       | """.stripMargin)

    assert(frame.columns.sameElements(Array("name", "age", "age_sma")))
    // Retrieve the results
    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] =
      Array(
        Row("Jake", 70, null),
        Row("Hello", 30, null),
        Row("John", 25, 41.666666666666664),
        Row("Jane", 20, 25))
    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val nameField = UnresolvedAttribute("name")
    val ageField = UnresolvedAttribute("age")
    val ageSmaField = UnresolvedAttribute("age_sma")
    val sort = Sort(Seq(SortOrder(ageField, Descending)), global = true, table)
    val countWindow = new WindowExpression(
      UnresolvedFunction("COUNT", Seq(Literal(1)), isDistinct = false),
      WindowSpecDefinition(Seq(), Seq(), SpecifiedWindowFrame(RowFrame, Literal(-2), CurrentRow)))
    val smaWindow = WindowExpression(
      UnresolvedFunction("AVG", Seq(ageField), isDistinct = false),
      WindowSpecDefinition(Seq(), Seq(), SpecifiedWindowFrame(RowFrame, Literal(-2), CurrentRow)))
    val caseWhen = CaseWhen(Seq((LessThan(countWindow, Literal(3)), Literal(null))), smaWindow)
    val trendlineProjectList = Seq(UnresolvedStar(None), Alias(caseWhen, "age_sma")())
    val expectedPlan =
      Project(Seq(nameField, ageField, ageSmaField), Project(trendlineProjectList, sort))
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test multiple trendline sma commands") {
    val frame = sql(s"""
                       | source = $testTable | trendline sort + age sma(2, age) as two_points_sma sma(3, age) as three_points_sma | fields name, age, two_points_sma, three_points_sma
                       | """.stripMargin)

    assert(frame.columns.sameElements(Array("name", "age", "two_points_sma", "three_points_sma")))
    // Retrieve the results
    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] =
      Array(
        Row("Jane", 20, null, null),
        Row("John", 25, 22.5, null),
        Row("Hello", 30, 27.5, 25.0),
        Row("Jake", 70, 50.0, 41.666666666666664))
    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val nameField = UnresolvedAttribute("name")
    val ageField = UnresolvedAttribute("age")
    val ageTwoPointsSmaField = UnresolvedAttribute("two_points_sma")
    val ageThreePointsSmaField = UnresolvedAttribute("three_points_sma")
    val sort = Sort(Seq(SortOrder(ageField, Ascending)), global = true, table)
    val twoPointsCountWindow = new WindowExpression(
      UnresolvedFunction("COUNT", Seq(Literal(1)), isDistinct = false),
      WindowSpecDefinition(Seq(), Seq(), SpecifiedWindowFrame(RowFrame, Literal(-1), CurrentRow))
    )
    val twoPointsSmaWindow = WindowExpression(
      UnresolvedFunction("AVG", Seq(ageField), isDistinct = false),
      WindowSpecDefinition(Seq(), Seq(), SpecifiedWindowFrame(RowFrame, Literal(-1), CurrentRow)))
    val threePointsCountWindow = new WindowExpression(
      UnresolvedFunction("COUNT", Seq(Literal(1)), isDistinct = false),
      WindowSpecDefinition(Seq(), Seq(), SpecifiedWindowFrame(RowFrame, Literal(-2), CurrentRow))
    )
    val threePointsSmaWindow = WindowExpression(
      UnresolvedFunction("AVG", Seq(ageField), isDistinct = false),
      WindowSpecDefinition(Seq(), Seq(), SpecifiedWindowFrame(RowFrame, Literal(-2), CurrentRow)))
    val twoPointsCaseWhen = CaseWhen(Seq((LessThan(twoPointsCountWindow, Literal(2)), Literal(null))), twoPointsSmaWindow)
    val threePointsCaseWhen = CaseWhen(Seq((LessThan(threePointsCountWindow, Literal(3)), Literal(null))), threePointsSmaWindow)
    val trendlineProjectList = Seq(
      UnresolvedStar(None),
      Alias(twoPointsCaseWhen, "two_points_sma")(),
      Alias(threePointsCaseWhen, "three_points_sma")())
    val expectedPlan = Project(
      Seq(nameField, ageField, ageTwoPointsSmaField, ageThreePointsSmaField),
      Project(trendlineProjectList, sort))
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test trendline sma command on evaluated column") {
    val frame = sql(s"""
                       | source = $testTable | eval doubled_age = age * 2 | trendline sort + age sma(2, doubled_age) as doubled_age_sma | fields name, doubled_age, doubled_age_sma
                       | """.stripMargin)

    assert(frame.columns.sameElements(Array("name", "doubled_age", "doubled_age_sma")))
    // Retrieve the results
    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] =
      Array(
        Row("Jane", 40, null),
        Row("John", 50, 45.0),
        Row("Hello", 60, 55.0),
        Row("Jake", 140, 100.0))
    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val nameField = UnresolvedAttribute("name")
    val ageField = UnresolvedAttribute("age")
    val doubledAgeField = UnresolvedAttribute("doubled_age")
    val doubledAgeSmaField = UnresolvedAttribute("doubled_age_sma")
    val evalProject = Project(
      Seq(
        UnresolvedStar(None),
        Alias(
          UnresolvedFunction("*", Seq(ageField, Literal(2)), isDistinct = false),
          "doubled_age")()),
      table)
    val sort = Sort(Seq(SortOrder(ageField, Ascending)), global = true, evalProject)
    val countWindow = new WindowExpression(
      UnresolvedFunction("COUNT", Seq(Literal(1)), isDistinct = false),
      WindowSpecDefinition(Seq(), Seq(), SpecifiedWindowFrame(RowFrame, Literal(-1), CurrentRow))
    )
    val doubleAgeSmaWindow = WindowExpression(
      UnresolvedFunction("AVG", Seq(doubledAgeField), isDistinct = false),
      WindowSpecDefinition(Seq(), Seq(), SpecifiedWindowFrame(RowFrame, Literal(-1), CurrentRow)))
    val caseWhen = CaseWhen(Seq((LessThan(countWindow, Literal(2)), Literal(null))), doubleAgeSmaWindow)
    val trendlineProjectList =
      Seq(UnresolvedStar(None), Alias(caseWhen, "doubled_age_sma")())
    val expectedPlan = Project(
      Seq(nameField, doubledAgeField, doubledAgeSmaField),
      Project(trendlineProjectList, sort))
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }
}
