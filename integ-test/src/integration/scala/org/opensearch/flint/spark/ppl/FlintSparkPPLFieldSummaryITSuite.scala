/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.flint.spark.ppl

import org.opensearch.sql.ppl.utils.DataTypeTransformer.seq

import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{Alias, Ascending, Expression, Literal, NamedExpression, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.streaming.StreamTest

class FlintSparkPPLFieldSummaryITSuite
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

  test("test fieldsummary with single field includefields(status_code) & nulls=true ") {
    val frame = sql(s"""
                       | source = $testTable | fieldsummary includefields= status_code nulls=true
                       | """.stripMargin)
    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] =
      Array(Row("status_code", 4, 3, 200, 403, 276.0, "int"))
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    val logicalPlan: LogicalPlan = frame.queryExecution.logical

    // Aggregate with functions applied to status_code
    val aggregateExpressions: Seq[NamedExpression] = Seq(
      Alias(Literal("status_code"), "Field")(),
      Alias(
        UnresolvedFunction("COUNT", Seq(UnresolvedAttribute("status_code")), isDistinct = false),
        "COUNT")(),
      Alias(
        UnresolvedFunction("COUNT", Seq(UnresolvedAttribute("status_code")), isDistinct = true),
        "COUNT_DISTINCT")(),
      Alias(
        UnresolvedFunction("MIN", Seq(UnresolvedAttribute("status_code")), isDistinct = false),
        "MIN")(),
      Alias(
        UnresolvedFunction("MAX", Seq(UnresolvedAttribute("status_code")), isDistinct = false),
        "MAX")(),
      Alias(
        UnresolvedFunction("AVG", Seq(UnresolvedAttribute("status_code")), isDistinct = false),
        "AVG")(),
      Alias(
        UnresolvedFunction("TYPEOF", Seq(UnresolvedAttribute("status_code")), isDistinct = false),
        "TYPEOF")())

    val table =
      UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))

    // Define the aggregate plan with alias for TYPEOF in the aggregation
    val aggregatePlan = Aggregate(
      groupingExpressions = Seq(Alias(
        UnresolvedFunction("TYPEOF", Seq(UnresolvedAttribute("status_code")), isDistinct = false),
        "TYPEOF")()),
      aggregateExpressions,
      table)
    val expectedPlan = Project(seq(UnresolvedStar(None)), aggregatePlan)
    // Compare the two plans
    comparePlans(expectedPlan, logicalPlan, false)
  }

  /**
   * // val frame = sql(s""" // | SELECT // | 'status_code' AS Field, // | COUNT(status_code) AS
   * Count, // | COUNT(DISTINCT status_code) AS Distinct, // | MIN(status_code) AS Min, // |
   * MAX(status_code) AS Max, // | AVG(CAST(status_code AS DOUBLE)) AS Avg, // |
   * typeof(status_code) AS Type, // | (SELECT COLLECT_LIST(STRUCT(status_code, count_status)) //
   * \| FROM ( // | SELECT status_code, COUNT(*) AS count_status // | FROM $testTable // | GROUP
   * BY status_code // | ORDER BY count_status DESC // | LIMIT 5 // | )) AS top_values, // |
   * COUNT(*) - COUNT(status_code) AS Nulls // | FROM $testTable // | GROUP BY typeof(status_code)
   * // | // | UNION ALL // | // | SELECT // | 'id' AS Field, // | COUNT(id) AS Count, // |
   * COUNT(DISTINCT id) AS Distinct, // | MIN(id) AS Min, // | MAX(id) AS Max, // | AVG(CAST(id AS
   * DOUBLE)) AS Avg, // | typeof(id) AS Type, // | (SELECT COLLECT_LIST(STRUCT(id, count_id)) //
   * \| FROM ( // | SELECT id, COUNT(*) AS count_id // | FROM $testTable // | GROUP BY id // |
   * ORDER BY count_id DESC // | LIMIT 5 // | )) AS top_values, // | COUNT(*) - COUNT(id) AS Nulls
   * // | FROM $testTable // | GROUP BY typeof(id) // |""".stripMargin) // Aggregate with
   * functions applied to status_code
   */
  test(
    "test fieldsummary with single field includefields(id, status_code, request_path) & nulls=true") {
    val frame = sql(s"""
                       | source = $testTable | fieldsummary includefields= id, status_code, request_path nulls=true
                       | """.stripMargin)

    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] =
      Array(
        Row("id", 6L, 6L, "1", "6", 3.5, "int"),
        Row("status_code", 4L, 3L, "200", "403", 276.0, "int"),
        Row("request_path", 4L, 3L, "/about", "/home", null, "string"))

    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    val logicalPlan: LogicalPlan = frame.queryExecution.logical

    val table =
      UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    // Define the aggregate plan with alias for TYPEOF in the aggregation
    val aggregateIdPlan = Aggregate(
      Seq(
        Alias(
          UnresolvedFunction("TYPEOF", Seq(UnresolvedAttribute("id")), isDistinct = false),
          "TYPEOF")()),
      Seq(
        Alias(Literal("id"), "Field")(),
        Alias(
          UnresolvedFunction("COUNT", Seq(UnresolvedAttribute("id")), isDistinct = false),
          "COUNT")(),
        Alias(
          UnresolvedFunction("COUNT", Seq(UnresolvedAttribute("id")), isDistinct = true),
          "COUNT_DISTINCT")(),
        Alias(
          UnresolvedFunction("MIN", Seq(UnresolvedAttribute("id")), isDistinct = false),
          "MIN")(),
        Alias(
          UnresolvedFunction("MAX", Seq(UnresolvedAttribute("id")), isDistinct = false),
          "MAX")(),
        Alias(
          UnresolvedFunction("AVG", Seq(UnresolvedAttribute("id")), isDistinct = false),
          "AVG")(),
        Alias(
          UnresolvedFunction("TYPEOF", Seq(UnresolvedAttribute("id")), isDistinct = false),
          "TYPEOF")()),
      table)
    val idProj = Project(seq(UnresolvedStar(None)), aggregateIdPlan)

    // Aggregate with functions applied to status_code
    // Define the aggregate plan with alias for TYPEOF in the aggregation
    val aggregateStatusCodePlan = Aggregate(
      Seq(Alias(
        UnresolvedFunction("TYPEOF", Seq(UnresolvedAttribute("status_code")), isDistinct = false),
        "TYPEOF")()),
      Seq(
        Alias(Literal("status_code"), "Field")(),
        Alias(
          UnresolvedFunction(
            "COUNT",
            Seq(UnresolvedAttribute("status_code")),
            isDistinct = false),
          "COUNT")(),
        Alias(
          UnresolvedFunction("COUNT", Seq(UnresolvedAttribute("status_code")), isDistinct = true),
          "COUNT_DISTINCT")(),
        Alias(
          UnresolvedFunction("MIN", Seq(UnresolvedAttribute("status_code")), isDistinct = false),
          "MIN")(),
        Alias(
          UnresolvedFunction("MAX", Seq(UnresolvedAttribute("status_code")), isDistinct = false),
          "MAX")(),
        Alias(
          UnresolvedFunction("AVG", Seq(UnresolvedAttribute("status_code")), isDistinct = false),
          "AVG")(),
        Alias(
          UnresolvedFunction(
            "TYPEOF",
            Seq(UnresolvedAttribute("status_code")),
            isDistinct = false),
          "TYPEOF")()),
      table)
    val statusCodeProj =
      Project(seq(UnresolvedStar(None)), aggregateStatusCodePlan)

    // Define the aggregate plan with alias for TYPEOF in the aggregation
    val aggregatePlan = Aggregate(
      Seq(
        Alias(
          UnresolvedFunction(
            "TYPEOF",
            Seq(UnresolvedAttribute("request_path")),
            isDistinct = false),
          "TYPEOF")()),
      Seq(
        Alias(Literal("request_path"), "Field")(),
        Alias(
          UnresolvedFunction(
            "COUNT",
            Seq(UnresolvedAttribute("request_path")),
            isDistinct = false),
          "COUNT")(),
        Alias(
          UnresolvedFunction(
            "COUNT",
            Seq(UnresolvedAttribute("request_path")),
            isDistinct = true),
          "COUNT_DISTINCT")(),
        Alias(
          UnresolvedFunction("MIN", Seq(UnresolvedAttribute("request_path")), isDistinct = false),
          "MIN")(),
        Alias(
          UnresolvedFunction("MAX", Seq(UnresolvedAttribute("request_path")), isDistinct = false),
          "MAX")(),
        Alias(
          UnresolvedFunction("AVG", Seq(UnresolvedAttribute("request_path")), isDistinct = false),
          "AVG")(),
        Alias(
          UnresolvedFunction(
            "TYPEOF",
            Seq(UnresolvedAttribute("request_path")),
            isDistinct = false),
          "TYPEOF")()),
      table)
    val requestPathProj = Project(seq(UnresolvedStar(None)), aggregatePlan)

    val expectedPlan =
      Union(seq(idProj, statusCodeProj, requestPathProj), true, true)
    // Compare the two plans
    comparePlans(expectedPlan, logicalPlan, false)
  }

}
