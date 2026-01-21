/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.flint.spark.ppl

import org.opensearch.flint.spark.ppl.legacy.ppl.utils.DataTypeTransformer.seq

import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{Alias, Ascending, EqualTo, Expression, Literal, NamedExpression, Not, SortOrder, Subtract}
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
      Array(Row("status_code", 4, 3, 200, 403, 184.0, 184.0, 161.16699413961905, 2, "int"))
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
        "DISTINCT")(),
      Alias(
        UnresolvedFunction("MIN", Seq(UnresolvedAttribute("status_code")), isDistinct = false),
        "MIN")(),
      Alias(
        UnresolvedFunction("MAX", Seq(UnresolvedAttribute("status_code")), isDistinct = false),
        "MAX")(),
      Alias(
        UnresolvedFunction(
          "AVG",
          Seq(
            UnresolvedFunction(
              "COALESCE",
              Seq(UnresolvedAttribute("status_code"), Literal(0)),
              isDistinct = false)),
          isDistinct = false),
        "AVG")(),
      Alias(
        UnresolvedFunction(
          "MEAN",
          Seq(
            UnresolvedFunction(
              "COALESCE",
              Seq(UnresolvedAttribute("status_code"), Literal(0)),
              isDistinct = false)),
          isDistinct = false),
        "MEAN")(),
      Alias(
        UnresolvedFunction(
          "STDDEV",
          Seq(
            UnresolvedFunction(
              "COALESCE",
              Seq(UnresolvedAttribute("status_code"), Literal(0)),
              isDistinct = false)),
          isDistinct = false),
        "STDDEV")(),
      Alias(
        Subtract(
          UnresolvedFunction("COUNT", Seq(Literal(1)), isDistinct = false),
          UnresolvedFunction(
            "COUNT",
            Seq(UnresolvedAttribute("status_code")),
            isDistinct = false)),
        "Nulls")(),
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

  test("test fieldsummary with single field includefields(status_code) & nulls=false ") {
    val frame = sql(s"""
                       | source = $testTable | fieldsummary includefields= status_code nulls=false
                       | """.stripMargin)
    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] =
      Array(Row("status_code", 4, 3, 200, 403, 276.0, 276.0, 97.1356439899038, 2, "int"))
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
        "DISTINCT")(),
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
        UnresolvedFunction("MEAN", Seq(UnresolvedAttribute("status_code")), isDistinct = false),
        "MEAN")(),
      Alias(
        UnresolvedFunction("STDDEV", Seq(UnresolvedAttribute("status_code")), isDistinct = false),
        "STDDEV")(),
      Alias(
        Subtract(
          UnresolvedFunction("COUNT", Seq(Literal(1)), isDistinct = false),
          UnresolvedFunction(
            "COUNT",
            Seq(UnresolvedAttribute("status_code")),
            isDistinct = false)),
        "Nulls")(),
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

  test(
    "test fieldsummary with single field includefields(status_code) & nulls=true with a where filter ") {
    val frame = sql(s"""
                       | source = $testTable | where status_code != 200 | fieldsummary includefields= status_code nulls=true
                       | """.stripMargin)
    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] =
      Array(Row("status_code", 2, 2, 301, 403, 352.0, 352.0, 72.12489168102785, 0, "int"))
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
        "DISTINCT")(),
      Alias(
        UnresolvedFunction("MIN", Seq(UnresolvedAttribute("status_code")), isDistinct = false),
        "MIN")(),
      Alias(
        UnresolvedFunction("MAX", Seq(UnresolvedAttribute("status_code")), isDistinct = false),
        "MAX")(),
      Alias(
        UnresolvedFunction(
          "AVG",
          Seq(
            UnresolvedFunction(
              "COALESCE",
              Seq(UnresolvedAttribute("status_code"), Literal(0)),
              isDistinct = false)),
          isDistinct = false),
        "AVG")(),
      Alias(
        UnresolvedFunction(
          "MEAN",
          Seq(
            UnresolvedFunction(
              "COALESCE",
              Seq(UnresolvedAttribute("status_code"), Literal(0)),
              isDistinct = false)),
          isDistinct = false),
        "MEAN")(),
      Alias(
        UnresolvedFunction(
          "STDDEV",
          Seq(
            UnresolvedFunction(
              "COALESCE",
              Seq(UnresolvedAttribute("status_code"), Literal(0)),
              isDistinct = false)),
          isDistinct = false),
        "STDDEV")(),
      Alias(
        Subtract(
          UnresolvedFunction("COUNT", Seq(Literal(1)), isDistinct = false),
          UnresolvedFunction(
            "COUNT",
            Seq(UnresolvedAttribute("status_code")),
            isDistinct = false)),
        "Nulls")(),
      Alias(
        UnresolvedFunction("TYPEOF", Seq(UnresolvedAttribute("status_code")), isDistinct = false),
        "TYPEOF")())

    val table =
      UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))

    val filterCondition = Not(EqualTo(UnresolvedAttribute("status_code"), Literal(200)))
    val aggregatePlan = Aggregate(
      groupingExpressions = Seq(Alias(
        UnresolvedFunction("TYPEOF", Seq(UnresolvedAttribute("status_code")), isDistinct = false),
        "TYPEOF")()),
      aggregateExpressions,
      Filter(filterCondition, table))

    val expectedPlan = Project(Seq(UnresolvedStar(None)), aggregatePlan)
    // Compare the two plans
    comparePlans(expectedPlan, logicalPlan, false)
  }

  test(
    "test fieldsummary with single field includefields(status_code) & nulls=false with a where filter ") {
    val frame = sql(s"""
                       | source = $testTable | where status_code != 200 | fieldsummary includefields= status_code nulls=false
                       | """.stripMargin)
    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] =
      Array(Row("status_code", 2, 2, 301, 403, 352.0, 352.0, 72.12489168102785, 0, "int"))
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
        "DISTINCT")(),
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
        UnresolvedFunction("MEAN", Seq(UnresolvedAttribute("status_code")), isDistinct = false),
        "MEAN")(),
      Alias(
        UnresolvedFunction("STDDEV", Seq(UnresolvedAttribute("status_code")), isDistinct = false),
        "STDDEV")(),
      Alias(
        Subtract(
          UnresolvedFunction("COUNT", Seq(Literal(1)), isDistinct = false),
          UnresolvedFunction(
            "COUNT",
            Seq(UnresolvedAttribute("status_code")),
            isDistinct = false)),
        "Nulls")(),
      Alias(
        UnresolvedFunction("TYPEOF", Seq(UnresolvedAttribute("status_code")), isDistinct = false),
        "TYPEOF")())

    val table =
      UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))

    val filterCondition = Not(EqualTo(UnresolvedAttribute("status_code"), Literal(200)))
    val aggregatePlan = Aggregate(
      groupingExpressions = Seq(Alias(
        UnresolvedFunction("TYPEOF", Seq(UnresolvedAttribute("status_code")), isDistinct = false),
        "TYPEOF")()),
      aggregateExpressions,
      Filter(filterCondition, table))

    val expectedPlan = Project(Seq(UnresolvedStar(None)), aggregatePlan)
    // Compare the two plans
    comparePlans(expectedPlan, logicalPlan, false)
  }

  test(
    "test fieldsummary with single field includefields(id, status_code, request_path) & nulls=true") {
    val frame = sql(s"""
                       | source = $testTable | fieldsummary includefields= id, status_code, request_path nulls=true
                       | """.stripMargin)

    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] =
      Array(
        Row("id", 6L, 6L, "1", "6", 3.5, 3.5, 1.8708286933869707, 0, "int"),
        Row("status_code", 4L, 3L, "200", "403", 184.0, 184.0, 161.16699413961905, 2, "int"),
        Row("request_path", 4L, 3L, "/about", "/home", 0.0, 0.0, 0.0, 2, "string"))

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
          "DISTINCT")(),
        Alias(
          UnresolvedFunction("MIN", Seq(UnresolvedAttribute("id")), isDistinct = false),
          "MIN")(),
        Alias(
          UnresolvedFunction("MAX", Seq(UnresolvedAttribute("id")), isDistinct = false),
          "MAX")(),
        Alias(
          UnresolvedFunction(
            "AVG",
            Seq(
              UnresolvedFunction(
                "COALESCE",
                Seq(UnresolvedAttribute("id"), Literal(0)),
                isDistinct = false)),
            isDistinct = false),
          "AVG")(),
        Alias(
          UnresolvedFunction(
            "MEAN",
            Seq(
              UnresolvedFunction(
                "COALESCE",
                Seq(UnresolvedAttribute("id"), Literal(0)),
                isDistinct = false)),
            isDistinct = false),
          "MEAN")(),
        Alias(
          UnresolvedFunction(
            "STDDEV",
            Seq(
              UnresolvedFunction(
                "COALESCE",
                Seq(UnresolvedAttribute("id"), Literal(0)),
                isDistinct = false)),
            isDistinct = false),
          "STDDEV")(),
        Alias(
          Subtract(
            UnresolvedFunction("COUNT", Seq(Literal(1)), isDistinct = false),
            UnresolvedFunction("COUNT", Seq(UnresolvedAttribute("id")), isDistinct = false)),
          "Nulls")(),
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
          "DISTINCT")(),
        Alias(
          UnresolvedFunction("MIN", Seq(UnresolvedAttribute("status_code")), isDistinct = false),
          "MIN")(),
        Alias(
          UnresolvedFunction("MAX", Seq(UnresolvedAttribute("status_code")), isDistinct = false),
          "MAX")(),
        Alias(
          UnresolvedFunction(
            "AVG",
            Seq(
              UnresolvedFunction(
                "COALESCE",
                Seq(UnresolvedAttribute("status_code"), Literal(0)),
                isDistinct = false)),
            isDistinct = false),
          "AVG")(),
        Alias(
          UnresolvedFunction(
            "MEAN",
            Seq(
              UnresolvedFunction(
                "COALESCE",
                Seq(UnresolvedAttribute("status_code"), Literal(0)),
                isDistinct = false)),
            isDistinct = false),
          "MEAN")(),
        Alias(
          UnresolvedFunction(
            "STDDEV",
            Seq(
              UnresolvedFunction(
                "COALESCE",
                Seq(UnresolvedAttribute("status_code"), Literal(0)),
                isDistinct = false)),
            isDistinct = false),
          "STDDEV")(),
        Alias(
          Subtract(
            UnresolvedFunction("COUNT", Seq(Literal(1)), isDistinct = false),
            UnresolvedFunction(
              "COUNT",
              Seq(UnresolvedAttribute("status_code")),
              isDistinct = false)),
          "Nulls")(),
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
          "DISTINCT")(),
        Alias(
          UnresolvedFunction("MIN", Seq(UnresolvedAttribute("request_path")), isDistinct = false),
          "MIN")(),
        Alias(
          UnresolvedFunction("MAX", Seq(UnresolvedAttribute("request_path")), isDistinct = false),
          "MAX")(),
        Alias(
          UnresolvedFunction(
            "AVG",
            Seq(
              UnresolvedFunction(
                "COALESCE",
                Seq(UnresolvedAttribute("request_path"), Literal(0)),
                isDistinct = false)),
            isDistinct = false),
          "AVG")(),
        Alias(
          UnresolvedFunction(
            "MEAN",
            Seq(
              UnresolvedFunction(
                "COALESCE",
                Seq(UnresolvedAttribute("request_path"), Literal(0)),
                isDistinct = false)),
            isDistinct = false),
          "MEAN")(),
        Alias(
          UnresolvedFunction(
            "STDDEV",
            Seq(
              UnresolvedFunction(
                "COALESCE",
                Seq(UnresolvedAttribute("request_path"), Literal(0)),
                isDistinct = false)),
            isDistinct = false),
          "STDDEV")(),
        Alias(
          Subtract(
            UnresolvedFunction("COUNT", Seq(Literal(1)), isDistinct = false),
            UnresolvedFunction(
              "COUNT",
              Seq(UnresolvedAttribute("request_path")),
              isDistinct = false)),
          "Nulls")(),
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

  test(
    "test fieldsummary with single field includefields(id, status_code, request_path) & nulls=false") {
    val frame = sql(s"""
                       | source = $testTable | fieldsummary includefields= id, status_code, request_path nulls=false
                       | """.stripMargin)

    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] =
      Array(
        Row("id", 6L, 6L, "1", "6", 3.5, 3.5, 1.8708286933869707, 0, "int"),
        Row("status_code", 4L, 3L, "200", "403", 276.0, 276.0, 97.1356439899038, 2, "int"),
        Row("request_path", 4L, 3L, "/about", "/home", null, null, null, 2, "string"))

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
          "DISTINCT")(),
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
          UnresolvedFunction("MEAN", Seq(UnresolvedAttribute("id")), isDistinct = false),
          "MEAN")(),
        Alias(
          UnresolvedFunction("STDDEV", Seq(UnresolvedAttribute("id")), isDistinct = false),
          "STDDEV")(),
        Alias(
          Subtract(
            UnresolvedFunction("COUNT", Seq(Literal(1)), isDistinct = false),
            UnresolvedFunction("COUNT", Seq(UnresolvedAttribute("id")), isDistinct = false)),
          "Nulls")(),
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
          "DISTINCT")(),
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
          UnresolvedFunction("MEAN", Seq(UnresolvedAttribute("status_code")), isDistinct = false),
          "MEAN")(),
        Alias(
          UnresolvedFunction(
            "STDDEV",
            Seq(UnresolvedAttribute("status_code")),
            isDistinct = false),
          "STDDEV")(),
        Alias(
          Subtract(
            UnresolvedFunction("COUNT", Seq(Literal(1)), isDistinct = false),
            UnresolvedFunction(
              "COUNT",
              Seq(UnresolvedAttribute("status_code")),
              isDistinct = false)),
          "Nulls")(),
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
          "DISTINCT")(),
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
            "MEAN",
            Seq(UnresolvedAttribute("request_path")),
            isDistinct = false),
          "MEAN")(),
        Alias(
          UnresolvedFunction(
            "STDDEV",
            Seq(UnresolvedAttribute("request_path")),
            isDistinct = false),
          "STDDEV")(),
        Alias(
          Subtract(
            UnresolvedFunction("COUNT", Seq(Literal(1)), isDistinct = false),
            UnresolvedFunction(
              "COUNT",
              Seq(UnresolvedAttribute("request_path")),
              isDistinct = false)),
          "Nulls")(),
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
