/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.flint.spark.ppl

import org.opensearch.sql.ppl.utils.DataTypeTransformer.seq

import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{Alias, Ascending, Expression, Literal, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, DataFrameDropColumns, LogicalPlan, Project, Sort}
import org.apache.spark.sql.streaming.StreamTest

class FlintSparkPPLFillnullITSuite
    extends QueryTest
    with LogicalPlanTestUtils
    with FlintPPLSuite
    with StreamTest {

  private val testTable = "spark_catalog.default.flint_ppl_test"

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Create test table
    createNullableTableHttpLog(testTable)
  }

  protected override def afterEach(): Unit = {
    super.afterEach()
    // Stop all streaming jobs if any
    spark.streams.active.foreach { job =>
      job.stop()
      job.awaitTermination()
    }
  }

  test("test fillnull with one null replacement value and one column") {
    val frame = sql(s"""
                       | source = $testTable | fillnull with 0 in status_code
                       | """.stripMargin)

    assert(frame.columns.sameElements(Array("id", "request_path", "timestamp", "status_code")))
    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] =
      Array(
        Row(1, "/home", null, 200),
        Row(2, "/about", "2023-10-01 10:05:00", 0),
        Row(3, "/contact", "2023-10-01 10:10:00", 0),
        Row(4, null, "2023-10-01 10:15:00", 301),
        Row(5, null, "2023-10-01 10:20:00", 200),
        Row(6, "/home", null, 403))
    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Int](_.getAs[Int](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val expectedPlan = fillNullExpectedPlan(Seq(("status_code", Literal(0))))
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test fillnull with various null replacement values and one column") {
    val frame = sql(s"""
                       | source = $testTable | fillnull using status_code=101
                       | """.stripMargin)

    assert(frame.columns.sameElements(Array("id", "request_path", "timestamp", "status_code")))
    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] =
      Array(
        Row(1, "/home", null, 200),
        Row(2, "/about", "2023-10-01 10:05:00", 101),
        Row(3, "/contact", "2023-10-01 10:10:00", 101),
        Row(4, null, "2023-10-01 10:15:00", 301),
        Row(5, null, "2023-10-01 10:20:00", 200),
        Row(6, "/home", null, 403))
    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Int](_.getAs[Int](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val expectedPlan = fillNullExpectedPlan(Seq(("status_code", Literal(101))))
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test fillnull with one null replacement value and two columns") {
    val frame = sql(s"""
                       | source = $testTable | fillnull with concat('??', '?') in request_path, timestamp | fields id, request_path, timestamp
                       | """.stripMargin)

    assert(frame.columns.sameElements(Array("id", "request_path", "timestamp")))
    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] =
      Array(
        Row(1, "/home", "???"),
        Row(2, "/about", "2023-10-01 10:05:00"),
        Row(3, "/contact", "2023-10-01 10:10:00"),
        Row(4, "???", "2023-10-01 10:15:00"),
        Row(5, "???", "2023-10-01 10:20:00"),
        Row(6, "/home", "???"))
    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Int](_.getAs[Int](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val fillNullPlan = fillNullExpectedPlan(
      Seq(
        (
          "request_path",
          UnresolvedFunction("concat", Seq(Literal("??"), Literal("?")), isDistinct = false)),
        (
          "timestamp",
          UnresolvedFunction("concat", Seq(Literal("??"), Literal("?")), isDistinct = false))),
      addDefaultProject = false)
    val expectedPlan = Project(
      Seq(
        UnresolvedAttribute("id"),
        UnresolvedAttribute("request_path"),
        UnresolvedAttribute("timestamp")),
      fillNullPlan)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test fillnull with various null replacement values and two columns") {
    val frame = sql(s"""
                       | source = $testTable | fillnull using request_path=upper('/not_found'), timestamp='*' | fields id, request_path, timestamp
                       | """.stripMargin)

    assert(frame.columns.sameElements(Array("id", "request_path", "timestamp")))
    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] =
      Array(
        Row(1, "/home", "*"),
        Row(2, "/about", "2023-10-01 10:05:00"),
        Row(3, "/contact", "2023-10-01 10:10:00"),
        Row(4, "/NOT_FOUND", "2023-10-01 10:15:00"),
        Row(5, "/NOT_FOUND", "2023-10-01 10:20:00"),
        Row(6, "/home", "*"))
    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Int](_.getAs[Int](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val fillNullPlan = fillNullExpectedPlan(
      Seq(
        (
          "request_path",
          UnresolvedFunction("upper", Seq(Literal("/not_found")), isDistinct = false)),
        ("timestamp", Literal("*"))),
      addDefaultProject = false)
    val expectedPlan = Project(
      Seq(
        UnresolvedAttribute("id"),
        UnresolvedAttribute("request_path"),
        UnresolvedAttribute("timestamp")),
      fillNullPlan)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test fillnull with one null replacement value and stats and sort command") {
    val frame = sql(s"""
                       | source = $testTable | fillnull with 500 in status_code
                       | |  stats count(status_code) by status_code, request_path
                       | |  sort request_path, status_code
                       | """.stripMargin)

    assert(frame.columns.sameElements(Array("count(status_code)", "status_code", "request_path")))
    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] =
      Array(
        Row(1, 200, null),
        Row(1, 301, null),
        Row(1, 500, "/about"),
        Row(1, 500, "/contact"),
        Row(1, 200, "/home"),
        Row(1, 403, "/home"))
    // Compare the results
    assert(results.sameElements(expectedResults))

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val fillNullPlan =
      fillNullExpectedPlan(Seq(("status_code", Literal(500))), addDefaultProject = false)
    val aggregateExpressions =
      Seq(
        Alias(
          UnresolvedFunction(
            Seq("COUNT"),
            Seq(UnresolvedAttribute("status_code")),
            isDistinct = false),
          "count(status_code)")(),
        Alias(UnresolvedAttribute("status_code"), "status_code")(),
        Alias(UnresolvedAttribute("request_path"), "request_path")())
    val aggregatePlan = Aggregate(
      Seq(
        Alias(UnresolvedAttribute("status_code"), "status_code")(),
        Alias(UnresolvedAttribute("request_path"), "request_path")()),
      aggregateExpressions,
      fillNullPlan)
    val sortPlan = Sort(
      Seq(
        SortOrder(UnresolvedAttribute("request_path"), Ascending),
        SortOrder(UnresolvedAttribute("status_code"), Ascending)),
      global = true,
      aggregatePlan)
    val expectedPlan = Project(seq(UnresolvedStar(None)), sortPlan)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test fillnull with various null replacement value and stats and sort command") {
    val frame = sql(s"""
                       | source = $testTable | fillnull using  status_code = 500, request_path = '/home'
                       | |  stats count(status_code) by status_code, request_path
                       | |  sort request_path, status_code
                       | """.stripMargin)

    assert(frame.columns.sameElements(Array("count(status_code)", "status_code", "request_path")))
    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] =
      Array(
        Row(1, 500, "/about"),
        Row(1, 500, "/contact"),
        Row(2, 200, "/home"),
        Row(1, 301, "/home"),
        Row(1, 403, "/home"))
    // Compare the results
    assert(results.sameElements(expectedResults))

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val fillNullPlan = fillNullExpectedPlan(
      Seq(("status_code", Literal(500)), ("request_path", Literal("/home"))),
      addDefaultProject = false)
    val aggregateExpressions =
      Seq(
        Alias(
          UnresolvedFunction(
            Seq("COUNT"),
            Seq(UnresolvedAttribute("status_code")),
            isDistinct = false),
          "count(status_code)")(),
        Alias(UnresolvedAttribute("status_code"), "status_code")(),
        Alias(UnresolvedAttribute("request_path"), "request_path")())
    val aggregatePlan = Aggregate(
      Seq(
        Alias(UnresolvedAttribute("status_code"), "status_code")(),
        Alias(UnresolvedAttribute("request_path"), "request_path")()),
      aggregateExpressions,
      fillNullPlan)
    val sortPlan = Sort(
      Seq(
        SortOrder(UnresolvedAttribute("request_path"), Ascending),
        SortOrder(UnresolvedAttribute("status_code"), Ascending)),
      global = true,
      aggregatePlan)
    val expectedPlan = Project(seq(UnresolvedStar(None)), sortPlan)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test fillnull with one null replacement value and missing columns") {
    val ex = intercept[AnalysisException](sql(s"""
                       | source = $testTable | fillnull with '!!!' in
                       | """.stripMargin))

    assert(ex.getMessage().contains("Syntax error "))
  }

  test("test fillnull with various null replacement values and missing columns") {
    val ex = intercept[AnalysisException](sql(s"""
                                                 | source = $testTable | fillnull using
                                                 | """.stripMargin))

    assert(ex.getMessage().contains("Syntax error "))
  }

  test("test fillnull with null_replacement type mismatch") {
    val frame = sql(s"""
                       | source = $testTable | fillnull with cast(0 as long) in status_code
                       | """.stripMargin)

    assert(frame.columns.sameElements(Array("id", "request_path", "timestamp", "status_code")))
    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] =
      Array(
        Row(1, "/home", null, 200),
        Row(2, "/about", "2023-10-01 10:05:00", 0),
        Row(3, "/contact", "2023-10-01 10:10:00", 0),
        Row(4, null, "2023-10-01 10:15:00", 301),
        Row(5, null, "2023-10-01 10:20:00", 200),
        Row(6, "/home", null, 403))
    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Int](_.getAs[Int](0))
    assert(results.sorted.sameElements(expectedResults.sorted))
  }

  private def fillNullExpectedPlan(
      nullReplacements: Seq[(String, Expression)],
      addDefaultProject: Boolean = true): LogicalPlan = {
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val renameProjectList = UnresolvedStar(None) +: nullReplacements.map {
      case (nullableColumn, nullReplacement) =>
        Alias(
          UnresolvedFunction(
            "coalesce",
            Seq(UnresolvedAttribute(nullableColumn), nullReplacement),
            isDistinct = false),
          nullableColumn)()
    }
    val renameProject = Project(renameProjectList, table)
    val droppedColumns =
      nullReplacements.map(_._1).map(columnName => UnresolvedAttribute(columnName))
    val dropSourceColumn = DataFrameDropColumns(droppedColumns, renameProject)
    if (addDefaultProject) {
      Project(seq(UnresolvedStar(None)), dropSourceColumn)
    } else {
      dropSourceColumn
    }
  }
}
