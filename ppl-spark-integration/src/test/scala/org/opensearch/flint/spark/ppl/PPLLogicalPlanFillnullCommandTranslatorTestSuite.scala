/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import org.opensearch.flint.spark.ppl.PlaneUtils.plan
import org.opensearch.flint.spark.ppl.legacy.ppl.{CatalystPlanContext, CatalystQueryPlanVisitor}
import org.opensearch.flint.spark.ppl.legacy.ppl.utils.DataTypeTransformer.seq
import org.scalatest.matchers.should.Matchers

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{Alias, Literal, NamedExpression}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{DataFrameDropColumns, Project}

class PPLLogicalPlanFillnullCommandTranslatorTestSuite
    extends SparkFunSuite
    with PlanTest
    with LogicalPlanTestUtils
    with Matchers {

  private val planTransformer = new CatalystQueryPlanVisitor()
  private val pplParser = new PPLSyntaxParser()

  test("test fillnull with one null replacement value and one column") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(
          pplParser,
          "source=relation | fillnull with 'null replacement value' in column_name"),
        context)

    val relation = UnresolvedRelation(Seq("relation"))

    val renameProjectList: Seq[NamedExpression] =
      Seq(
        UnresolvedStar(None),
        Alias(
          UnresolvedFunction(
            "coalesce",
            Seq(UnresolvedAttribute("column_name"), Literal("null replacement value")),
            isDistinct = false),
          "column_name")())
    val renameProject = Project(renameProjectList, relation)

    val dropSourceColumn =
      DataFrameDropColumns(Seq(UnresolvedAttribute("column_name")), renameProject)

    val expectedPlan = Project(seq(UnresolvedStar(None)), dropSourceColumn)
    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }

  test("test fillnull with one null replacement value, one column and function invocation") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(pplParser, "source=relation | fillnull with upper(another_field) in column_name"),
        context)

    val relation = UnresolvedRelation(Seq("relation"))

    val renameProjectList: Seq[NamedExpression] =
      Seq(
        UnresolvedStar(None),
        Alias(
          UnresolvedFunction(
            "coalesce",
            Seq(
              UnresolvedAttribute("column_name"),
              UnresolvedFunction(
                "upper",
                Seq(UnresolvedAttribute("another_field")),
                isDistinct = false)),
            isDistinct = false),
          "column_name")())
    val renameProject = Project(renameProjectList, relation)

    val dropSourceColumn =
      DataFrameDropColumns(Seq(UnresolvedAttribute("column_name")), renameProject)

    val expectedPlan = Project(seq(UnresolvedStar(None)), dropSourceColumn)
    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }

  test("test fillnull with one null replacement value and multiple column") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(
          pplParser,
          "source=relation | fillnull with 'another null replacement value' in column_name_one, column_name_two, column_name_three"),
        context)

    val relation = UnresolvedRelation(Seq("relation"))

    val renameProjectList: Seq[NamedExpression] = Seq(
      UnresolvedStar(None),
      Alias(
        UnresolvedFunction(
          "coalesce",
          Seq(UnresolvedAttribute("column_name_one"), Literal("another null replacement value")),
          isDistinct = false),
        "column_name_one")(),
      Alias(
        UnresolvedFunction(
          "coalesce",
          Seq(UnresolvedAttribute("column_name_two"), Literal("another null replacement value")),
          isDistinct = false),
        "column_name_two")(),
      Alias(
        UnresolvedFunction(
          "coalesce",
          Seq(
            UnresolvedAttribute("column_name_three"),
            Literal("another null replacement value")),
          isDistinct = false),
        "column_name_three")())
    val renameProject = Project(renameProjectList, relation)

    val dropSourceColumn = DataFrameDropColumns(
      Seq(
        UnresolvedAttribute("column_name_one"),
        UnresolvedAttribute("column_name_two"),
        UnresolvedAttribute("column_name_three")),
      renameProject)

    val expectedPlan = Project(seq(UnresolvedStar(None)), dropSourceColumn)
    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }

  test("test fillnull with possibly various null replacement value and one column") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(pplParser, "source=relation | fillnull using column_name='null replacement value'"),
        context)

    val relation = UnresolvedRelation(Seq("relation"))

    val renameProjectList: Seq[NamedExpression] =
      Seq(
        UnresolvedStar(None),
        Alias(
          UnresolvedFunction(
            "coalesce",
            Seq(UnresolvedAttribute("column_name"), Literal("null replacement value")),
            isDistinct = false),
          "column_name")())
    val renameProject = Project(renameProjectList, relation)

    val dropSourceColumn =
      DataFrameDropColumns(Seq(UnresolvedAttribute("column_name")), renameProject)

    val expectedPlan = Project(seq(UnresolvedStar(None)), dropSourceColumn)
    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }

  test(
    "test fillnull with possibly various null replacement value, one column and function invocation") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(
          pplParser,
          "source=relation | fillnull using column_name=concat('missing value for', id)"),
        context)

    val relation = UnresolvedRelation(Seq("relation"))

    val renameProjectList: Seq[NamedExpression] =
      Seq(
        UnresolvedStar(None),
        Alias(
          UnresolvedFunction(
            "coalesce",
            Seq(
              UnresolvedAttribute("column_name"),
              UnresolvedFunction(
                "concat",
                Seq(Literal("missing value for"), UnresolvedAttribute("id")),
                isDistinct = false)),
            isDistinct = false),
          "column_name")())
    val renameProject = Project(renameProjectList, relation)

    val dropSourceColumn =
      DataFrameDropColumns(Seq(UnresolvedAttribute("column_name")), renameProject)

    val expectedPlan = Project(seq(UnresolvedStar(None)), dropSourceColumn)
    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }

  test("test fillnull with possibly various null replacement value and three columns") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(
          pplParser,
          "source=relation | fillnull using column_name_1='null replacement value 1', column_name_2='null replacement value 2', column_name_3='null replacement value 3'"),
        context)

    val relation = UnresolvedRelation(Seq("relation"))

    val renameProjectList: Seq[NamedExpression] = Seq(
      UnresolvedStar(None),
      Alias(
        UnresolvedFunction(
          "coalesce",
          Seq(UnresolvedAttribute("column_name_1"), Literal("null replacement value 1")),
          isDistinct = false),
        "column_name_1")(),
      Alias(
        UnresolvedFunction(
          "coalesce",
          Seq(UnresolvedAttribute("column_name_2"), Literal("null replacement value 2")),
          isDistinct = false),
        "column_name_2")(),
      Alias(
        UnresolvedFunction(
          "coalesce",
          Seq(UnresolvedAttribute("column_name_3"), Literal("null replacement value 3")),
          isDistinct = false),
        "column_name_3")())
    val renameProject = Project(renameProjectList, relation)

    val dropSourceColumn = DataFrameDropColumns(
      Seq(
        UnresolvedAttribute("column_name_1"),
        UnresolvedAttribute("column_name_2"),
        UnresolvedAttribute("column_name_3")),
      renameProject)

    val expectedPlan = Project(seq(UnresolvedStar(None)), dropSourceColumn)
    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }
}
