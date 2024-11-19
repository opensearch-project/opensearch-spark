/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{Alias, Ascending, Descending, Literal, NamedExpression, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.command.DescribeTableCommand
import org.apache.spark.sql.streaming.StreamTest

class FlintSparkPPLTopAndRareITSuite
    extends QueryTest
    with LogicalPlanTestUtils
    with FlintPPLSuite
    with StreamTest {

  /** Test table and index name */
  private val testTable = "spark_catalog.default.flint_ppl_test"
  private val newTestTable = "spark_catalog.default.new_flint_ppl_test"

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Create test tables
    createOccupationTopRareTable(newTestTable)
    createPartitionedMultiRowAddressTable(testTable)
  }

  protected override def afterEach(): Unit = {
    super.afterEach()
    // Stop all streaming jobs if any
    spark.streams.active.foreach { job =>
      job.stop()
      job.awaitTermination()
    }
  }

  test("create ppl rare address field query test") {
    val frame = sql(s"""
         | source = $testTable| rare address
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    assert(results.length == 3)

    val expectedRow = Row(1, "Vancouver")
    assert(
      results.head == expectedRow,
      s"Expected least frequent result to be $expectedRow, but got ${results.head}")

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val addressField = UnresolvedAttribute("address")
    val projectList: Seq[NamedExpression] = Seq(UnresolvedStar(None))

    val aggregateExpressions = Seq(
      Alias(
        UnresolvedFunction(Seq("COUNT"), Seq(addressField), isDistinct = false),
        "count_address")(),
      addressField)
    val aggregatePlan =
      Aggregate(
        Seq(addressField),
        aggregateExpressions,
        UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test")))
    val sortedPlan: LogicalPlan =
      Sort(
        Seq(
          SortOrder(
            Alias(
              UnresolvedFunction(Seq("COUNT"), Seq(addressField), isDistinct = false),
              "count_address")(),
            Ascending)),
        global = true,
        aggregatePlan)
    val expectedPlan = Project(projectList, sortedPlan)
    comparePlans(expectedPlan, logicalPlan, checkAnalysis = false)
  }

  test("create ppl rare address field query test with approximation") {
    val frame = sql(s"""
         | source = $testTable| rare_approx address
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    assert(results.length == 3)

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val addressField = UnresolvedAttribute("address")
    val projectList: Seq[NamedExpression] = Seq(UnresolvedStar(None))

    val aggregateExpressions = Seq(
      Alias(
        UnresolvedFunction(Seq("APPROX_COUNT_DISTINCT"), Seq(addressField), isDistinct = false),
        "count_address")(),
      addressField)
    val aggregatePlan =
      Aggregate(
        Seq(addressField),
        aggregateExpressions,
        UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test")))
    val sortedPlan: LogicalPlan =
      Sort(
        Seq(
          SortOrder(
            Alias(
              UnresolvedFunction(
                Seq("APPROX_COUNT_DISTINCT"),
                Seq(addressField),
                isDistinct = false),
              "count_address")(),
            Ascending)),
        global = true,
        aggregatePlan)
    val expectedPlan = Project(projectList, sortedPlan)
    comparePlans(expectedPlan, logicalPlan, checkAnalysis = false)
  }

  test("create ppl rare address by age field query test") {
    val frame = sql(s"""
         | source = $testTable| rare address by age
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    assert(results.length == 5)

    val expectedRow = Row(1, "Vancouver", 60)
    assert(
      results.head == expectedRow,
      s"Expected least frequent result to be $expectedRow, but got ${results.head}")

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val addressField = UnresolvedAttribute("address")
    val ageField = UnresolvedAttribute("age")
    val ageAlias = Alias(ageField, "age")()

    val projectList: Seq[NamedExpression] = Seq(UnresolvedStar(None))

    val countExpr = Alias(
      UnresolvedFunction(Seq("COUNT"), Seq(addressField), isDistinct = false),
      "count_address")()

    val aggregateExpressions = Seq(countExpr, addressField, ageAlias)
    val aggregatePlan =
      Aggregate(
        Seq(addressField, ageAlias),
        aggregateExpressions,
        UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test")))

    val sortedPlan: LogicalPlan =
      Sort(
        Seq(
          SortOrder(
            Alias(
              UnresolvedFunction(Seq("COUNT"), Seq(addressField), isDistinct = false),
              "count_address")(),
            Ascending)),
        global = true,
        aggregatePlan)

    val expectedPlan = Project(projectList, sortedPlan)
    comparePlans(expectedPlan, logicalPlan, false)
  }

  test("create ppl rare 3 address by age field query test") {
    val frame = sql(s"""
         | source = $testTable| rare 3 address by age
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    assert(results.length == 3)

    val expectedRow = Row(1, "Vancouver", 60)
    assert(
      results.head == expectedRow,
      s"Expected least frequent result to be $expectedRow, but got ${results.head}")

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val addressField = UnresolvedAttribute("address")
    val ageField = UnresolvedAttribute("age")
    val ageAlias = Alias(ageField, "age")()

    val projectList: Seq[NamedExpression] = Seq(UnresolvedStar(None))

    val countExpr = Alias(
      UnresolvedFunction(Seq("COUNT"), Seq(addressField), isDistinct = false),
      "count_address")()

    val aggregateExpressions = Seq(countExpr, addressField, ageAlias)
    val aggregatePlan =
      Aggregate(
        Seq(addressField, ageAlias),
        aggregateExpressions,
        UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test")))

    val sortedPlan: LogicalPlan =
      Sort(
        Seq(
          SortOrder(
            Alias(
              UnresolvedFunction(Seq("COUNT"), Seq(addressField), isDistinct = false),
              "count_address")(),
            Ascending)),
        global = true,
        aggregatePlan)

    val planWithLimit =
      GlobalLimit(Literal(3), LocalLimit(Literal(3), sortedPlan))
    val expectedPlan = Project(Seq(UnresolvedStar(None)), planWithLimit)
    comparePlans(expectedPlan, logicalPlan, false)
  }

  test("create ppl rare 3 address by age field query test with approximation") {
    val frame = sql(s"""
         | source = $testTable| rare_approx 3 address by age
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    assert(results.length == 3)

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val addressField = UnresolvedAttribute("address")
    val ageField = UnresolvedAttribute("age")
    val ageAlias = Alias(ageField, "age")()

    val projectList: Seq[NamedExpression] = Seq(UnresolvedStar(None))

    val countExpr = Alias(
      UnresolvedFunction(Seq("APPROX_COUNT_DISTINCT"), Seq(addressField), isDistinct = false),
      "count_address")()

    val aggregateExpressions = Seq(countExpr, addressField, ageAlias)
    val aggregatePlan =
      Aggregate(
        Seq(addressField, ageAlias),
        aggregateExpressions,
        UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test")))

    val sortedPlan: LogicalPlan =
      Sort(
        Seq(
          SortOrder(
            Alias(
              UnresolvedFunction(
                Seq("APPROX_COUNT_DISTINCT"),
                Seq(addressField),
                isDistinct = false),
              "count_address")(),
            Ascending)),
        global = true,
        aggregatePlan)

    val planWithLimit =
      GlobalLimit(Literal(3), LocalLimit(Literal(3), sortedPlan))
    val expectedPlan = Project(Seq(UnresolvedStar(None)), planWithLimit)
    comparePlans(expectedPlan, logicalPlan, false)
  }

  test("create ppl top address field query test") {
    val frame = sql(s"""
         | source = $testTable| top address
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    assert(results.length == 3)

    val expectedRows = Set(Row(2, "Portland"), Row(2, "Seattle"))
    val actualRows = results.take(2).toSet

    // Compare the sets
    assert(
      actualRows == expectedRows,
      s"The first two results do not match the expected rows. Expected: $expectedRows, Actual: $actualRows")

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val addressField = UnresolvedAttribute("address")
    val projectList: Seq[NamedExpression] = Seq(UnresolvedStar(None))

    val aggregateExpressions = Seq(
      Alias(
        UnresolvedFunction(Seq("COUNT"), Seq(addressField), isDistinct = false),
        "count_address")(),
      addressField)
    val aggregatePlan =
      Aggregate(
        Seq(addressField),
        aggregateExpressions,
        UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test")))
    val sortedPlan: LogicalPlan =
      Sort(
        Seq(
          SortOrder(
            Alias(
              UnresolvedFunction(Seq("COUNT"), Seq(addressField), isDistinct = false),
              "count_address")(),
            Descending)),
        global = true,
        aggregatePlan)
    val expectedPlan = Project(projectList, sortedPlan)
    comparePlans(expectedPlan, logicalPlan, checkAnalysis = false)
  }

  test("create ppl top address field query test  with approximation") {
    val frame = sql(s"""
         | source = $testTable| top_approx address
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    assert(results.length == 3)

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val addressField = UnresolvedAttribute("address")
    val projectList: Seq[NamedExpression] = Seq(UnresolvedStar(None))

    val aggregateExpressions = Seq(
      Alias(
        UnresolvedFunction(Seq("APPROX_COUNT_DISTINCT"), Seq(addressField), isDistinct = false),
        "count_address")(),
      addressField)
    val aggregatePlan =
      Aggregate(
        Seq(addressField),
        aggregateExpressions,
        UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test")))
    val sortedPlan: LogicalPlan =
      Sort(
        Seq(
          SortOrder(
            Alias(
              UnresolvedFunction(
                Seq("APPROX_COUNT_DISTINCT"),
                Seq(addressField),
                isDistinct = false),
              "count_address")(),
            Descending)),
        global = true,
        aggregatePlan)
    val expectedPlan = Project(projectList, sortedPlan)
    comparePlans(expectedPlan, logicalPlan, checkAnalysis = false)
  }

  test("create ppl top 3 countries query test") {
    val frame = sql(s"""
         | source = $newTestTable| top 3 country
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    assert(results.length == 3)

    val expectedRows = Set(Row(6, "Canada"), Row(3, "USA"), Row(1, "England"))
    val actualRows = results.take(3).toSet

    // Compare the sets
    assert(
      actualRows == expectedRows,
      s"The first two results do not match the expected rows. Expected: $expectedRows, Actual: $actualRows")

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val countryField = UnresolvedAttribute("country")
    val countExpr = Alias(
      UnresolvedFunction(Seq("COUNT"), Seq(countryField), isDistinct = false),
      "count_country")()
    val aggregateExpressions = Seq(countExpr, countryField)
    val aggregatePlan =
      Aggregate(
        Seq(countryField),
        aggregateExpressions,
        UnresolvedRelation(Seq("spark_catalog", "default", "new_flint_ppl_test")))

    val sortedPlan: LogicalPlan =
      Sort(
        Seq(
          SortOrder(
            Alias(
              UnresolvedFunction(Seq("COUNT"), Seq(countryField), isDistinct = false),
              "count_country")(),
            Descending)),
        global = true,
        aggregatePlan)

    val planWithLimit =
      GlobalLimit(Literal(3), LocalLimit(Literal(3), sortedPlan))
    val expectedPlan = Project(Seq(UnresolvedStar(None)), planWithLimit)
    comparePlans(expectedPlan, logicalPlan, checkAnalysis = false)
  }

  test("create ppl top 3 countries query test with approximation") {
    val frame = sql(s"""
         | source = $newTestTable| top_approx 3 country
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    assert(results.length == 3)

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val countryField = UnresolvedAttribute("country")
    val countExpr = Alias(
      UnresolvedFunction(Seq("APPROX_COUNT_DISTINCT"), Seq(countryField), isDistinct = false),
      "count_country")()
    val aggregateExpressions = Seq(countExpr, countryField)
    val aggregatePlan =
      Aggregate(
        Seq(countryField),
        aggregateExpressions,
        UnresolvedRelation(Seq("spark_catalog", "default", "new_flint_ppl_test")))

    val sortedPlan: LogicalPlan =
      Sort(
        Seq(
          SortOrder(
            Alias(
              UnresolvedFunction(
                Seq("APPROX_COUNT_DISTINCT"),
                Seq(countryField),
                isDistinct = false),
              "count_country")(),
            Descending)),
        global = true,
        aggregatePlan)

    val planWithLimit =
      GlobalLimit(Literal(3), LocalLimit(Literal(3), sortedPlan))
    val expectedPlan = Project(Seq(UnresolvedStar(None)), planWithLimit)
    comparePlans(expectedPlan, logicalPlan, checkAnalysis = false)
  }

  test("create ppl top 2 countries by occupation field query test") {
    val frame = sql(s"""
         | source = $newTestTable| top 3 country by occupation
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    assert(results.length == 3)

    val expectedRows =
      Set(Row(3, "Canada", "Doctor"), Row(2, "Canada", "Scientist"), Row(2, "USA", "Engineer"))
    val actualRows = results.take(3).toSet

    // Compare the sets
    assert(
      actualRows == expectedRows,
      s"The first two results do not match the expected rows. Expected: $expectedRows, Actual: $actualRows")

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val countryField = UnresolvedAttribute("country")
    val occupationField = UnresolvedAttribute("occupation")
    val occupationFieldAlias = Alias(occupationField, "occupation")()

    val countExpr = Alias(
      UnresolvedFunction(Seq("COUNT"), Seq(countryField), isDistinct = false),
      "count_country")()
    val aggregateExpressions = Seq(countExpr, countryField, occupationFieldAlias)
    val aggregatePlan =
      Aggregate(
        Seq(countryField, occupationFieldAlias),
        aggregateExpressions,
        UnresolvedRelation(Seq("spark_catalog", "default", "new_flint_ppl_test")))

    val sortedPlan: LogicalPlan =
      Sort(
        Seq(
          SortOrder(
            Alias(
              UnresolvedFunction(Seq("COUNT"), Seq(countryField), isDistinct = false),
              "count_country")(),
            Descending)),
        global = true,
        aggregatePlan)

    val planWithLimit =
      GlobalLimit(Literal(3), LocalLimit(Literal(3), sortedPlan))
    val expectedPlan = Project(Seq(UnresolvedStar(None)), planWithLimit)
    comparePlans(expectedPlan, logicalPlan, checkAnalysis = false)

  }

  test("create ppl top 2 countries by occupation field query test  with approximation") {
    val frame = sql(s"""
         | source = $newTestTable| top_approx 3 country by occupation
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    assert(results.length == 3)

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val countryField = UnresolvedAttribute("country")
    val occupationField = UnresolvedAttribute("occupation")
    val occupationFieldAlias = Alias(occupationField, "occupation")()

    val countExpr = Alias(
      UnresolvedFunction(Seq("APPROX_COUNT_DISTINCT"), Seq(countryField), isDistinct = false),
      "count_country")()
    val aggregateExpressions = Seq(countExpr, countryField, occupationFieldAlias)
    val aggregatePlan =
      Aggregate(
        Seq(countryField, occupationFieldAlias),
        aggregateExpressions,
        UnresolvedRelation(Seq("spark_catalog", "default", "new_flint_ppl_test")))

    val sortedPlan: LogicalPlan =
      Sort(
        Seq(
          SortOrder(
            Alias(
              UnresolvedFunction(
                Seq("APPROX_COUNT_DISTINCT"),
                Seq(countryField),
                isDistinct = false),
              "count_country")(),
            Descending)),
        global = true,
        aggregatePlan)

    val planWithLimit =
      GlobalLimit(Literal(3), LocalLimit(Literal(3), sortedPlan))
    val expectedPlan = Project(Seq(UnresolvedStar(None)), planWithLimit)
    comparePlans(expectedPlan, logicalPlan, checkAnalysis = false)

  }
}
