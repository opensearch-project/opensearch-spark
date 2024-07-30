/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import org.opensearch.sql.ppl.utils.DataTypeTransformer.seq

import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{Alias, Ascending, ExprId, Literal, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan, Project, Sort}
import org.apache.spark.sql.streaming.StreamTest

class FlintSparkPPLEvalITSuite
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

  test("test single eval expression with new field") {
    val frame = sql(s"""
         | source = $testTable | eval col = 1 | fields name, age
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
    val fieldsProjectList = Seq(UnresolvedAttribute("name"), UnresolvedAttribute("age"))
    val evalProjectList = Seq(
      UnresolvedStar(None),
      Alias(Literal(1), "col")(exprId = ExprId(0), qualifier = Seq.empty))
    val expectedPlan = Project(fieldsProjectList, Project(evalProjectList, table))
    // Compare the two plans
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test multiple eval expressions with new fields") {
    val frame = sql(s"""
         | source = $testTable | eval col1 = 1, col2 = 2 | fields name, age
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // results.foreach(println(_))
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
    val fieldsProjectList = Seq(UnresolvedAttribute("name"), UnresolvedAttribute("age"))
    val evalProjectList = Seq(
      UnresolvedStar(None),
      Alias(Literal(1), "col1")(exprId = ExprId(0), qualifier = Seq.empty),
      Alias(Literal(2), "col2")(exprId = ExprId(1), qualifier = Seq.empty))
    val expectedPlan = Project(fieldsProjectList, Project(evalProjectList, table))
    // Compare the two plans
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test eval expressions in fields list") {
    val frame = sql(s"""
         | source = $testTable | eval col1 = 1, col2 = 2 | fields name, age, col1, col2
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // results.foreach(println(_))
    // Define the expected results
    val expectedResults: Array[Row] = Array(
      Row("Jake", 70, 1, 2),
      Row("Hello", 30, 1, 2),
      Row("John", 25, 1, 2),
      Row("Jane", 20, 1, 2))
    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val fieldsProjectList = Seq(
      UnresolvedAttribute("name"),
      UnresolvedAttribute("age"),
      UnresolvedAttribute("col1"),
      UnresolvedAttribute("col2"))
    val evalProjectList = Seq(
      UnresolvedStar(None),
      Alias(Literal(1), "col1")(exprId = ExprId(0), qualifier = Seq.empty),
      Alias(Literal(2), "col2")(exprId = ExprId(1), qualifier = Seq.empty))
    val expectedPlan = Project(fieldsProjectList, Project(evalProjectList, table))
    // Compare the two plans
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test reuse existing fields in eval value with fields list") {
    val frame = sql(s"""
         | source = $testTable | eval col1 = state, col2 = country | fields name, age, col1, col2
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // results.foreach(println(_))
    // Define the expected results
    val expectedResults: Array[Row] = Array(
      Row("Jake", 70, "California", "USA"),
      Row("Hello", 30, "New York", "USA"),
      Row("John", 25, "Ontario", "Canada"),
      Row("Jane", 20, "Quebec", "Canada"))
    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val fieldsProjectList = Seq(
      UnresolvedAttribute("name"),
      UnresolvedAttribute("age"),
      UnresolvedAttribute("col1"),
      UnresolvedAttribute("col2"))
    val evalProjectList = Seq(
      UnresolvedStar(None),
      Alias(UnresolvedAttribute("state"), "col1")(exprId = ExprId(0), qualifier = Seq.empty),
      Alias(UnresolvedAttribute("country"), "col2")(exprId = ExprId(1), qualifier = Seq.empty))
    val expectedPlan = Project(fieldsProjectList, Project(evalProjectList, table))
    // Compare the two plans
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("fail when reuse existing fields in eval key") {
    val ex = intercept[AnalysisException](sql(s"""
             | source = $testTable | eval age = 40, new_field = 100 | fields name, age
             | """.stripMargin))
    assert(ex.getMessage().contains("Reference 'age' is ambiguous"))
  }

  test("test eval expression without fields command") {
    val frame = sql(s"""
         | source = $testTable | eval col1 = "New Field1", col2 = "New Field2"
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(
      Row("Jake", 70, "California", "USA", 2023, 4, "New Field1", "New Field2"),
      Row("Hello", 30, "New York", "USA", 2023, 4, "New Field1", "New Field2"),
      Row("John", 25, "Ontario", "Canada", 2023, 4, "New Field1", "New Field2"),
      Row("Jane", 20, "Quebec", "Canada", 2023, 4, "New Field1", "New Field2"))
    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val projectList = Seq(
      UnresolvedStar(None),
      Alias(Literal("New Field1"), "col1")(exprId = ExprId(0), qualifier = Seq.empty),
      Alias(Literal("New Field2"), "col2")(exprId = ExprId(1), qualifier = Seq.empty))
    val expectedPlan = Project(seq(UnresolvedStar(None)), Project(projectList, table))
    // Compare the two plans
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test multiple eval commands in fields list") {
    val frame = sql(s"""
         | source = $testTable | eval col1 = 1 | eval col2 = 2 | fields name, age, col1, col2
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // results.foreach(println(_))
    // Define the expected results
    val expectedResults: Array[Row] = Array(
      Row("Jake", 70, 1, 2),
      Row("Hello", 30, 1, 2),
      Row("John", 25, 1, 2),
      Row("Jane", 20, 1, 2))
    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val fieldsProjectList = Seq(
      UnresolvedAttribute("name"),
      UnresolvedAttribute("age"),
      UnresolvedAttribute("col1"),
      UnresolvedAttribute("col2"))
    val evalProjectList1 = Seq(
      UnresolvedStar(None),
      Alias(Literal(1), "col1")(exprId = ExprId(0), qualifier = Seq.empty))
    val evalProjectList2 = Seq(
      UnresolvedStar(None),
      Alias(Literal(2), "col2")(exprId = ExprId(1), qualifier = Seq.empty))
    val expectedPlan =
      Project(fieldsProjectList, Project(evalProjectList2, Project(evalProjectList1, table)))
    // Compare the two plans
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test multiple eval commands without fields list") {
    // Todo better to use function expression when function supported
    val frame = sql(s"""
         | source = $testTable | eval col1 = 1 | eval col2 = 2 | sort col1
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // results.foreach(println(_))
    // Define the expected results
    val expectedResults: Array[Row] = Array(
      Row("Jake", 70, "California", "USA", 2023, 4, 1, 2),
      Row("Hello", 30, "New York", "USA", 2023, 4, 1, 2),
      Row("John", 25, "Ontario", "Canada", 2023, 4, 1, 2),
      Row("Jane", 20, "Quebec", "Canada", 2023, 4, 1, 2))
    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val evalProjectList1 = Seq(
      UnresolvedStar(None),
      Alias(Literal(1), "col1")(exprId = ExprId(0), qualifier = Seq.empty))
    val evalProjectList2 = Seq(
      UnresolvedStar(None),
      Alias(Literal(2), "col2")(exprId = ExprId(1), qualifier = Seq.empty))
    val sortedPlan: LogicalPlan =
      Sort(
        Seq(SortOrder(UnresolvedAttribute("col1"), Ascending)),
        global = true,
        Project(evalProjectList2, Project(evalProjectList1, table)))
    val expectedPlan = Project(seq(UnresolvedStar(None)), sortedPlan)
    // Compare the two plans
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test complex eval commands - case 1") {
    val frame = sql(s"""
         | source = $testTable | eval col1 = 1 | sort col1 | head 4 | eval col2 = 2 | sort - col2 | sort age | head 2 | fields name, age, col2
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // results.foreach(println(_))
    // Define the expected results
    val expectedResults: Array[Row] = Array(Row("John", 25, 2), Row("Jane", 20, 2))
    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))
  }

  test("test complex eval commands - case 2") {
    val frame = sql(s"""
         | source = $testTable | eval col1 = age | sort - col1 | head 3 | eval col2 = age | sort + col2 | head 2
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // results.foreach(println(_))
    // Define the expected results
    val expectedResults: Array[Row] = Array(
      Row("Hello", 30, "New York", "USA", 2023, 4, 30, 30),
      Row("John", 25, "Ontario", "Canada", 2023, 4, 25, 25))
    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))
  }

  test("test complex eval commands - case 3") {
    val frame = sql(s"""
         | source = $testTable | eval col1 = age | sort - col1 | head 3 | eval col2 = age | fields name, age | sort + col2 | head 2
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // results.foreach(println(_))
    // Define the expected results
    val expectedResults: Array[Row] = Array(Row("Hello", 30), Row("John", 25))
    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))
  }

  test("test complex eval commands - case 4: execute 1, 2 and 3 together") {
    val frame1 = sql(s"""
         | source = $testTable | eval col1 = 1 | sort col1 | head 4 | eval col2 = 2 | sort - col2 | sort age | head 2 | fields name, age, col2
         | """.stripMargin)
    // Retrieve the results
    val results1: Array[Row] = frame1.collect()
    // results.foreach(println(_))
    // Define the expected results
    val expectedResults1: Array[Row] = Array(Row("John", 25, 2), Row("Jane", 20, 2))
    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results1.sorted.sameElements(expectedResults1.sorted))

    val frame2 = sql(s"""
         | source = $testTable | eval col1 = age | sort - col1 | head 3 | eval col2 = age | sort + col2 | head 2
         | """.stripMargin)
    // Retrieve the results
    val results2: Array[Row] = frame2.collect()
    // results1.foreach(println(_))
    // Define the expected results
    val expectedResults2: Array[Row] = Array(
      Row("Hello", 30, "New York", "USA", 2023, 4, 30, 30),
      Row("John", 25, "Ontario", "Canada", 2023, 4, 25, 25))
    // Compare the results
    assert(results2.sorted.sameElements(expectedResults2.sorted))

    val frame3 = sql(s"""
         | source = $testTable | eval col1 = age | sort - col1 | head 3 | eval col2 = age | fields name, age | sort + col2 | head 2
         | """.stripMargin)
    // Retrieve the results
    val results3: Array[Row] = frame3.collect()
    // results.foreach(println(_))
    // Define the expected results
    val expectedResults3: Array[Row] = Array(Row("Hello", 30), Row("John", 25))
    // Compare the results
    assert(results3.sorted.sameElements(expectedResults3.sorted))
  }

  test("test eval expression used in aggregation") {
    val frame = sql(s"""
         | source = $testTable | eval col1 = age | stats avg(col1)
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(Row(36.25))

    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Double](_.getAs[Double](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val evalProjectList = Seq(
      UnresolvedStar(None),
      Alias(UnresolvedAttribute("age"), "col1")(exprId = ExprId(0), qualifier = Seq.empty))
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val aggregateExpressions =
      Seq(
        Alias(
          UnresolvedFunction(Seq("AVG"), Seq(UnresolvedAttribute("col1")), isDistinct = false),
          "avg(col1)")())
    val aggregatePlan = Aggregate(Seq(), aggregateExpressions, Project(evalProjectList, table))
    val expectedPlan = Project(seq(UnresolvedStar(None)), aggregatePlan)

    // Compare the two plans
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  // +--------------------------------+
  // | Below tests are not supported  |
  // +--------------------------------+
  // Todo: Upgrade spark version to fix this test. This test failed due to a Spark bug of 3.3.
  ignore("test depended eval expressions with new field") {
    val frame = sql(s"""
         | source = $testTable | eval col1 = 1, col2 = col1 | fields name, age, col2
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // results.foreach(println(_))
    // Define the expected results
    val expectedResults: Array[Row] =
      Array(Row("Jake", 70, 1), Row("Hello", 30, 1), Row("John", 25, 1), Row("Jane", 20, 1))
    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val fieldsProjectList =
      Seq(UnresolvedAttribute("name"), UnresolvedAttribute("age"), UnresolvedAttribute("col2"))
    val evalProjectList = Seq(
      UnresolvedStar(None),
      Alias(Literal(1), "col1")(exprId = ExprId(0), qualifier = Seq.empty),
      Alias(UnresolvedAttribute("col1"), "col2")(exprId = ExprId(1), qualifier = Seq.empty))
    val expectedPlan = Project(fieldsProjectList, Project(evalProjectList, table))
    // Compare the two plans
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  // Todo function should work when the PR #448 merged.
  ignore("test complex eval expressions") {
    val frame = sql(s"""
         | source = $testTable | eval avg_name = avg(name) | fields name, age
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(
      Row("Jake", 70, 36.25),
      Row("Hello", 30, 36.25),
      Row("John", 25, 36.25),
      Row("Jane", 20, 36.25))
    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))
  }

  // Todo function should work when the PR #448 merged.
  ignore("test complex eval expressions without fields list") {
    val frame = sql(s"""
         | source = $testTable | eval col1 = "New Field", col2 = upper(col1)
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(
      Row("JAKE", 70, "California", "USA", 2023, 4, "New Field", "NEW FIELD"),
      Row("HELLO", 30, "New York", "USA", 2023, 4, "New Field", "NEW FIELD"),
      Row("JOHN", 25, "Ontario", "Canada", 2023, 4, "New Field", "NEW FIELD"),
      Row("JANE", 20, "Quebec", "Canada", 2023, 4, "New Field", "NEW FIELD"))
    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))
  }

  // Todo excluded fields not support yet
  ignore("test single eval expression with excluded fields") {
    val frame = sql(s"""
         | source = $testTable | eval new_field = "New Field" | fields - age
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(
      Row("Jake", "California", "USA", 2023, 4, "New Field"),
      Row("Hello", "New York", "USA", 2023, 4, "New Field"),
      Row("John", "Ontario", "Canada", 2023, 4, "New Field"),
      Row("Jane", "Quebec", "Canada", 2023, 4, "New Field"))
    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))
  }
}
