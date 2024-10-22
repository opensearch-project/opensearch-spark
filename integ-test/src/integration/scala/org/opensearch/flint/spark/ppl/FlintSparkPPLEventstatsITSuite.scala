/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{Alias, Divide, Floor, Literal, Multiply, RowFrame, SpecifiedWindowFrame, UnboundedFollowing, UnboundedPreceding, WindowExpression, WindowSpecDefinition}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project, Window}
import org.apache.spark.sql.streaming.StreamTest

class FlintSparkPPLEventstatsITSuite
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

  test("test eventstats avg") {
    val frame = sql(s"""
                       | source = $testTable | eventstats avg(age)
                       | """.stripMargin)
    val expected = Seq(
      Row("John", 25, "Ontario", "Canada", 2023, 4, 36.25),
      Row("Jane", 20, "Quebec", "Canada", 2023, 4, 36.25),
      Row("Jake", 70, "California", "USA", 2023, 4, 36.25),
      Row("Hello", 30, "New York", "USA", 2023, 4, 36.25))
    assertSameRows(expected, frame)

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val windowExpression = WindowExpression(
      UnresolvedFunction("AVG", Seq(UnresolvedAttribute("age")), isDistinct = false),
      WindowSpecDefinition(
        Nil,
        Nil,
        SpecifiedWindowFrame(RowFrame, UnboundedPreceding, UnboundedFollowing)))
    val avgWindowExprAlias = Alias(windowExpression, "avg(age)")()
    val windowPlan = Window(Seq(avgWindowExprAlias), Nil, Nil, table)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), windowPlan)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test eventstats avg, max, min, count") {
    val frame = sql(s"""
                       | source = $testTable | eventstats avg(age) as avg_age, max(age) as max_age, min(age) as min_age, count(age) as count
                       | """.stripMargin)
    val expected = Seq(
      Row("John", 25, "Ontario", "Canada", 2023, 4, 36.25, 70, 20, 4),
      Row("Jane", 20, "Quebec", "Canada", 2023, 4, 36.25, 70, 20, 4),
      Row("Jake", 70, "California", "USA", 2023, 4, 36.25, 70, 20, 4),
      Row("Hello", 30, "New York", "USA", 2023, 4, 36.25, 70, 20, 4))
    assertSameRows(expected, frame)

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val avgWindowExpression = WindowExpression(
      UnresolvedFunction("AVG", Seq(UnresolvedAttribute("age")), isDistinct = false),
      WindowSpecDefinition(
        Nil,
        Nil,
        SpecifiedWindowFrame(RowFrame, UnboundedPreceding, UnboundedFollowing)))
    val avgWindowExprAlias = Alias(avgWindowExpression, "avg_age")()

    val maxWindowExpression = WindowExpression(
      UnresolvedFunction("MAX", Seq(UnresolvedAttribute("age")), isDistinct = false),
      WindowSpecDefinition(
        Nil,
        Nil,
        SpecifiedWindowFrame(RowFrame, UnboundedPreceding, UnboundedFollowing)))
    val maxWindowExprAlias = Alias(maxWindowExpression, "max_age")()

    val minWindowExpression = WindowExpression(
      UnresolvedFunction("MIN", Seq(UnresolvedAttribute("age")), isDistinct = false),
      WindowSpecDefinition(
        Nil,
        Nil,
        SpecifiedWindowFrame(RowFrame, UnboundedPreceding, UnboundedFollowing)))
    val minWindowExprAlias = Alias(minWindowExpression, "min_age")()

    val countWindowExpression = WindowExpression(
      UnresolvedFunction("COUNT", Seq(UnresolvedAttribute("age")), isDistinct = false),
      WindowSpecDefinition(
        Nil,
        Nil,
        SpecifiedWindowFrame(RowFrame, UnboundedPreceding, UnboundedFollowing)))
    val countWindowExprAlias = Alias(countWindowExpression, "count")()
    val windowPlan = Window(
      Seq(avgWindowExprAlias, maxWindowExprAlias, minWindowExprAlias, countWindowExprAlias),
      Nil,
      Nil,
      table)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), windowPlan)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test eventstats avg by country") {
    val frame = sql(s"""
                       | source = $testTable | eventstats avg(age) by country
                       | """.stripMargin)
    val expected = Seq(
      Row("John", 25, "Ontario", "Canada", 2023, 4, 22.5),
      Row("Jane", 20, "Quebec", "Canada", 2023, 4, 22.5),
      Row("Jake", 70, "California", "USA", 2023, 4, 50),
      Row("Hello", 30, "New York", "USA", 2023, 4, 50))
    assertSameRows(expected, frame)

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val partitionSpec = Seq(Alias(UnresolvedAttribute("country"), "country")())
    val windowExpression = WindowExpression(
      UnresolvedFunction("AVG", Seq(UnresolvedAttribute("age")), isDistinct = false),
      WindowSpecDefinition(
        partitionSpec,
        Nil,
        SpecifiedWindowFrame(RowFrame, UnboundedPreceding, UnboundedFollowing)))
    val avgWindowExprAlias = Alias(windowExpression, "avg(age)")()
    val windowPlan = Window(Seq(avgWindowExprAlias), partitionSpec, Nil, table)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), windowPlan)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test eventstats avg, max, min, count by country") {
    val frame = sql(s"""
                       | source = $testTable | eventstats avg(age) as avg_age, max(age) as max_age, min(age) as min_age, count(age) as count by country
                       | """.stripMargin)
    val expected = Seq(
      Row("John", 25, "Ontario", "Canada", 2023, 4, 22.5, 25, 20, 2),
      Row("Jane", 20, "Quebec", "Canada", 2023, 4, 22.5, 25, 20, 2),
      Row("Jake", 70, "California", "USA", 2023, 4, 50, 70, 30, 2),
      Row("Hello", 30, "New York", "USA", 2023, 4, 50, 70, 30, 2))
    assertSameRows(expected, frame)

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val partitionSpec = Seq(Alias(UnresolvedAttribute("country"), "country")())
    val avgWindowExpression = WindowExpression(
      UnresolvedFunction("AVG", Seq(UnresolvedAttribute("age")), isDistinct = false),
      WindowSpecDefinition(
        partitionSpec,
        Nil,
        SpecifiedWindowFrame(RowFrame, UnboundedPreceding, UnboundedFollowing)))
    val avgWindowExprAlias = Alias(avgWindowExpression, "avg_age")()

    val maxWindowExpression = WindowExpression(
      UnresolvedFunction("MAX", Seq(UnresolvedAttribute("age")), isDistinct = false),
      WindowSpecDefinition(
        partitionSpec,
        Nil,
        SpecifiedWindowFrame(RowFrame, UnboundedPreceding, UnboundedFollowing)))
    val maxWindowExprAlias = Alias(maxWindowExpression, "max_age")()

    val minWindowExpression = WindowExpression(
      UnresolvedFunction("MIN", Seq(UnresolvedAttribute("age")), isDistinct = false),
      WindowSpecDefinition(
        partitionSpec,
        Nil,
        SpecifiedWindowFrame(RowFrame, UnboundedPreceding, UnboundedFollowing)))
    val minWindowExprAlias = Alias(minWindowExpression, "min_age")()

    val countWindowExpression = WindowExpression(
      UnresolvedFunction("COUNT", Seq(UnresolvedAttribute("age")), isDistinct = false),
      WindowSpecDefinition(
        partitionSpec,
        Nil,
        SpecifiedWindowFrame(RowFrame, UnboundedPreceding, UnboundedFollowing)))
    val countWindowExprAlias = Alias(countWindowExpression, "count")()
    val windowPlan = Window(
      Seq(avgWindowExprAlias, maxWindowExprAlias, minWindowExprAlias, countWindowExprAlias),
      partitionSpec,
      Nil,
      table)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), windowPlan)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test eventstats avg, max, min, count by span") {
    val frame = sql(s"""
                       | source = $testTable | eventstats avg(age) as avg_age, max(age) as max_age, min(age) as min_age, count(age) as count by span(age, 10) as age_span
                       | """.stripMargin)
    val expected = Seq(
      Row("John", 25, "Ontario", "Canada", 2023, 4, 22.5, 25, 20, 2),
      Row("Jane", 20, "Quebec", "Canada", 2023, 4, 22.5, 25, 20, 2),
      Row("Jake", 70, "California", "USA", 2023, 4, 70, 70, 70, 1),
      Row("Hello", 30, "New York", "USA", 2023, 4, 30, 30, 30, 1))
    assertSameRows(expected, frame)

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val span = Alias(
      Multiply(Floor(Divide(UnresolvedAttribute("age"), Literal(10))), Literal(10)),
      "age_span")()
    val partitionSpec = Seq(span)
    val windowExpression = WindowExpression(
      UnresolvedFunction("AVG", Seq(UnresolvedAttribute("age")), isDistinct = false),
      WindowSpecDefinition(
        partitionSpec,
        Nil,
        SpecifiedWindowFrame(RowFrame, UnboundedPreceding, UnboundedFollowing)))
    val avgWindowExprAlias = Alias(windowExpression, "avg_age")()

    val maxWindowExpression = WindowExpression(
      UnresolvedFunction("MAX", Seq(UnresolvedAttribute("age")), isDistinct = false),
      WindowSpecDefinition(
        partitionSpec,
        Nil,
        SpecifiedWindowFrame(RowFrame, UnboundedPreceding, UnboundedFollowing)))
    val maxWindowExprAlias = Alias(maxWindowExpression, "max_age")()

    val minWindowExpression = WindowExpression(
      UnresolvedFunction("MIN", Seq(UnresolvedAttribute("age")), isDistinct = false),
      WindowSpecDefinition(
        partitionSpec,
        Nil,
        SpecifiedWindowFrame(RowFrame, UnboundedPreceding, UnboundedFollowing)))
    val minWindowExprAlias = Alias(minWindowExpression, "min_age")()

    val countWindowExpression = WindowExpression(
      UnresolvedFunction("COUNT", Seq(UnresolvedAttribute("age")), isDistinct = false),
      WindowSpecDefinition(
        partitionSpec,
        Nil,
        SpecifiedWindowFrame(RowFrame, UnboundedPreceding, UnboundedFollowing)))
    val countWindowExprAlias = Alias(countWindowExpression, "count")()
    val windowPlan = Window(
      Seq(avgWindowExprAlias, maxWindowExprAlias, minWindowExprAlias, countWindowExprAlias),
      partitionSpec,
      Nil,
      table)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), windowPlan)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test eventstats avg, max, min, count by span and country") {
    val frame = sql(s"""
                       | source = $testTable | eventstats avg(age) as avg_age, max(age) as max_age, min(age) as min_age, count(age) as count by span(age, 10) as age_span, country
                       | """.stripMargin)
    val expected = Seq(
      Row("John", 25, "Ontario", "Canada", 2023, 4, 22.5, 25, 20, 2),
      Row("Jane", 20, "Quebec", "Canada", 2023, 4, 22.5, 25, 20, 2),
      Row("Jake", 70, "California", "USA", 2023, 4, 70, 70, 70, 1),
      Row("Hello", 30, "New York", "USA", 2023, 4, 30, 30, 30, 1))
    assertSameRows(expected, frame)
  }

  test("test eventstats avg, max, min, count by span and state") {
    val frame = sql(s"""
                       | source = $testTable | eventstats avg(age) as avg_age, max(age) as max_age, min(age) as min_age, count(age) as count by span(age, 10) as age_span, state
                       | """.stripMargin)
    val expected = Seq(
      Row("John", 25, "Ontario", "Canada", 2023, 4, 25, 25, 25, 1),
      Row("Jane", 20, "Quebec", "Canada", 2023, 4, 20, 20, 20, 1),
      Row("Jake", 70, "California", "USA", 2023, 4, 70, 70, 70, 1),
      Row("Hello", 30, "New York", "USA", 2023, 4, 30, 30, 30, 1))
    assertSameRows(expected, frame)
  }

  test("test eventstats stddev by span with filter") {
    val frame = sql(s"""
                       | source = $testTable | where country != 'USA' | eventstats stddev_samp(age) by span(age, 10) as age_span
                       | """.stripMargin)
    val expected = Seq(
      Row("John", 25, "Ontario", "Canada", 2023, 4, 3.5355339059327378),
      Row("Jane", 20, "Quebec", "Canada", 2023, 4, 3.5355339059327378))
    assertSameRows(expected, frame)
  }

  test("test eventstats stddev_pop by span with filter") {
    val frame = sql(s"""
                       | source = $testTable | where state != 'California' | eventstats stddev_pop(age) by span(age, 10) as age_span
                       | """.stripMargin)
    val expected = Seq(
      Row("John", 25, "Ontario", "Canada", 2023, 4, 2.5),
      Row("Jane", 20, "Quebec", "Canada", 2023, 4, 2.5),
      Row("Hello", 30, "New York", "USA", 2023, 4, 0.0))
    assertSameRows(expected, frame)
  }

  test("test eventstats percentile by span with filter") {
    val frame = sql(s"""
                       | source = $testTable | where state != 'California' | eventstats percentile_approx(age, 60) by span(age, 10) as age_span
                       | """.stripMargin)
    val expected = Seq(
      Row("John", 25, "Ontario", "Canada", 2023, 4, 25),
      Row("Jane", 20, "Quebec", "Canada", 2023, 4, 25),
      Row("Hello", 30, "New York", "USA", 2023, 4, 30))
    assertSameRows(expected, frame)
  }

  test("test multiple eventstats") {
    val frame = sql(s"""
                       | source = $testTable | eventstats avg(age) as avg_age by state, country | eventstats avg(avg_age) as avg_state_age by country
                       | """.stripMargin)
    val expected = Seq(
      Row("John", 25, "Ontario", "Canada", 2023, 4, 25.0, 22.5),
      Row("Jane", 20, "Quebec", "Canada", 2023, 4, 20.0, 22.5),
      Row("Jake", 70, "California", "USA", 2023, 4, 70.0, 50.0),
      Row("Hello", 30, "New York", "USA", 2023, 4, 30.0, 50.0))
    assertSameRows(expected, frame)

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val partitionSpec = Seq(
      Alias(UnresolvedAttribute("state"), "state")(),
      Alias(UnresolvedAttribute("country"), "country")())
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val avgAgeWindowExpression = WindowExpression(
      UnresolvedFunction("AVG", Seq(UnresolvedAttribute("age")), isDistinct = false),
      WindowSpecDefinition(
        partitionSpec,
        Nil,
        SpecifiedWindowFrame(RowFrame, UnboundedPreceding, UnboundedFollowing)))
    val avgAgeWindowExprAlias = Alias(avgAgeWindowExpression, "avg_age")()
    val windowPlan1 = Window(Seq(avgAgeWindowExprAlias), partitionSpec, Nil, table)

    val countryPartitionSpec = Seq(Alias(UnresolvedAttribute("country"), "country")())
    val avgStateAgeWindowExpression = WindowExpression(
      UnresolvedFunction("AVG", Seq(UnresolvedAttribute("avg_age")), isDistinct = false),
      WindowSpecDefinition(
        countryPartitionSpec,
        Nil,
        SpecifiedWindowFrame(RowFrame, UnboundedPreceding, UnboundedFollowing)))
    val avgStateAgeWindowExprAlias = Alias(avgStateAgeWindowExpression, "avg_state_age")()
    val windowPlan2 =
      Window(Seq(avgStateAgeWindowExprAlias), countryPartitionSpec, Nil, windowPlan1)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), windowPlan2)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test multiple eventstats with eval") {
    val frame = sql(s"""
                       | source = $testTable | eventstats avg(age) as avg_age by state, country | eval new_avg_age = avg_age - 10 | eventstats avg(new_avg_age) as avg_state_age by country
                       | """.stripMargin)
    val expected = Seq(
      Row("John", 25, "Ontario", "Canada", 2023, 4, 25.0, 15.0, 12.5),
      Row("Jane", 20, "Quebec", "Canada", 2023, 4, 20.0, 10.0, 12.5),
      Row("Jake", 70, "California", "USA", 2023, 4, 70.0, 60.0, 40.0),
      Row("Hello", 30, "New York", "USA", 2023, 4, 30.0, 20.0, 40.0))
    assertSameRows(expected, frame)
  }

  test("test multiple eventstats with eval and filter") {
    val frame = sql(s"""
                       | source = $testTable| eventstats avg(age) as avg_age by country, state, name | eval avg_age_divide_20 = avg_age - 20 | eventstats avg(avg_age_divide_20)
                       | as avg_state_age by country, state | where avg_state_age > 0 | eventstats count(avg_state_age) as count_country_age_greater_20 by country
                       | """.stripMargin)
    val expected = Seq(
      Row("John", 25, "Ontario", "Canada", 2023, 4, 25.0, 5.0, 5.0, 1),
      Row("Jake", 70, "California", "USA", 2023, 4, 70.0, 50.0, 50.0, 2),
      Row("Hello", 30, "New York", "USA", 2023, 4, 30.0, 10.0, 10.0, 2))
    assertSameRows(expected, frame)
  }

  test("test eventstats distinct_count by span with filter") {
    val exception = intercept[AnalysisException](sql(s"""
                       | source = $testTable | where state != 'California' | eventstats distinct_count(age) by span(age, 10) as age_span
                       | """.stripMargin))
    assert(exception.message.contains("Distinct window functions are not supported"))
  }
}
