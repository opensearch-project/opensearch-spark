/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import org.opensearch.flint.spark.ppl.PlaneUtils.plan
import org.opensearch.flint.spark.ppl.legacy.{CatalystPlanContext, CatalystQueryPlanVisitor}
import org.opensearch.flint.spark.ppl.legacy.utils.DataTypeTransformer.seq
import org.scalatest.matchers.should.Matchers

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{Alias, EqualTo, Literal, NamedExpression, Not, Subtract}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, Project, Union}

class PPLLogicalPlanFieldSummaryTranslatorTestSuite
    extends SparkFunSuite
    with PlanTest
    with LogicalPlanTestUtils
    with Matchers {

  private val planTransformer = new CatalystQueryPlanVisitor()
  private val pplParser = new PPLSyntaxParser()

  test("test fieldsummary with single field includefields(status_code) & nulls=false") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(pplParser, "source = t | fieldsummary includefields= status_code nulls=false"),
        context)

    // Define the table
    val table = UnresolvedRelation(Seq("t"))

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

    // Define the aggregate plan with alias for TYPEOF in the aggregation
    val aggregatePlan = Aggregate(
      groupingExpressions = Seq(Alias(
        UnresolvedFunction("TYPEOF", Seq(UnresolvedAttribute("status_code")), isDistinct = false),
        "TYPEOF")()),
      aggregateExpressions,
      table)
    val expectedPlan = Project(seq(UnresolvedStar(None)), aggregatePlan)
    // Compare the two plans
    comparePlans(expectedPlan, logPlan, false)
  }

  test("test fieldsummary with single field includefields(status_code) & nulls=true") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(pplParser, "source = t | fieldsummary includefields= status_code nulls=true"),
        context)

    // Define the table
    val table = UnresolvedRelation(Seq("t"))

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

    // Define the aggregate plan with alias for TYPEOF in the aggregation
    val aggregatePlan = Aggregate(
      groupingExpressions = Seq(Alias(
        UnresolvedFunction("TYPEOF", Seq(UnresolvedAttribute("status_code")), isDistinct = false),
        "TYPEOF")()),
      aggregateExpressions,
      table)
    val expectedPlan = Project(seq(UnresolvedStar(None)), aggregatePlan)
    // Compare the two plans
    comparePlans(expectedPlan, logPlan, false)
  }

  test(
    "test fieldsummary with single field includefields(status_code) & nulls=true with a where filter ") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(
          pplParser,
          "source = t | where status_code != 200 | fieldsummary includefields= status_code nulls=true"),
        context)

    // Define the table
    val table = UnresolvedRelation(Seq("t"))

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

    val filterCondition = Not(EqualTo(UnresolvedAttribute("status_code"), Literal(200)))
    val aggregatePlan = Aggregate(
      groupingExpressions = Seq(Alias(
        UnresolvedFunction("TYPEOF", Seq(UnresolvedAttribute("status_code")), isDistinct = false),
        "TYPEOF")()),
      aggregateExpressions,
      Filter(filterCondition, table))

    val expectedPlan = Project(Seq(UnresolvedStar(None)), aggregatePlan)

    // Compare the two plans
    comparePlans(expectedPlan, logPlan, false)
  }

  test(
    "test fieldsummary with single field includefields(status_code) & nulls=false with a where filter ") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(
          pplParser,
          "source = t | where status_code != 200 | fieldsummary includefields= status_code nulls=false"),
        context)

    // Define the table
    val table = UnresolvedRelation(Seq("t"))

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

    val filterCondition = Not(EqualTo(UnresolvedAttribute("status_code"), Literal(200)))
    val aggregatePlan = Aggregate(
      groupingExpressions = Seq(Alias(
        UnresolvedFunction("TYPEOF", Seq(UnresolvedAttribute("status_code")), isDistinct = false),
        "TYPEOF")()),
      aggregateExpressions,
      Filter(filterCondition, table))

    val expectedPlan = Project(Seq(UnresolvedStar(None)), aggregatePlan)

    // Compare the two plans
    comparePlans(expectedPlan, logPlan, false)
  }

  test(
    "test fieldsummary with single field includefields(id, status_code, request_path) & nulls=true") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(
          pplParser,
          "source = t | fieldsummary includefields= id, status_code, request_path nulls=true"),
        context)

    // Define the table
    val table = UnresolvedRelation(Seq("t"))

    // Aggregate with functions applied to status_code
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
    val statusCodeProj = Project(seq(UnresolvedStar(None)), aggregateStatusCodePlan)

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

    val expectedPlan = Union(seq(idProj, statusCodeProj, requestPathProj), true, true)
    // Compare the two plans
    comparePlans(expectedPlan, logPlan, false)
  }

  test(
    "test fieldsummary with single field includefields(id, status_code, request_path) & nulls=false") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(
          pplParser,
          "source = t | fieldsummary includefields= id, status_code, request_path nulls=false"),
        context)

    // Define the table
    val table = UnresolvedRelation(Seq("t"))

    // Aggregate with functions applied to status_code
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
    val statusCodeProj = Project(seq(UnresolvedStar(None)), aggregateStatusCodePlan)

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

    val expectedPlan = Union(seq(idProj, statusCodeProj, requestPathProj), true, true)
    // Compare the two plans
    comparePlans(expectedPlan, logPlan, false)
  }
}
