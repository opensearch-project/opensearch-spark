/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import org.opensearch.flint.spark.ppl.PlaneUtils.plan
import org.opensearch.sql.ppl.{CatalystPlanContext, CatalystQueryPlanVisitor}
import org.opensearch.sql.ppl.utils.{SortUtils, WindowSpecTransformer}
import org.scalatest.matchers.should.Matchers

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedRelation}
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Ascending, CurrentRow, IsNotNull, IsNull, LessThanOrEqual, Literal, NamedExpression, Or, RowFrame, RowNumber, SortOrder, SpecifiedWindowFrame, UnboundedPreceding, WindowExpression, WindowSpecDefinition}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{DataFrameDropColumns, Deduplicate, Filter, Project, Union, Window}

class PPLLogicalPlanDedupeTranslatorTestSuite
    extends SparkFunSuite
    with PlanTest
    with LogicalPlanTestUtils
    with Matchers {

  private val planTransformer = new CatalystQueryPlanVisitor()
  private val pplParser = new PPLSyntaxParser()

  test("test dedup a") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(plan(pplParser, "source=table | dedup a | fields a", false), context)

    val projectList: Seq[NamedExpression] = Seq(UnresolvedAttribute("a"))
    val filter = Filter(IsNotNull(UnresolvedAttribute("a")), UnresolvedRelation(Seq("table")))
    val deduplicate = Deduplicate(Seq(UnresolvedAttribute("a")), filter)
    val expectedPlan = Project(projectList, deduplicate)
    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }

  test("test dedup a, b, c") {
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(
      plan(pplParser, "source=table | dedup a, b, c | fields a, b, c", false),
      context)

    val projectList: Seq[NamedExpression] =
      Seq(UnresolvedAttribute("a"), UnresolvedAttribute("b"), UnresolvedAttribute("c"))
    val filter = Filter(
      And(
        And(IsNotNull(UnresolvedAttribute("a")), IsNotNull(UnresolvedAttribute("b"))),
        IsNotNull(UnresolvedAttribute("c"))),
      UnresolvedRelation(Seq("table")))
    val deduplicate = Deduplicate(
      Seq(UnresolvedAttribute("a"), UnresolvedAttribute("b"), UnresolvedAttribute("c")),
      filter)
    val expectedPlan = Project(projectList, deduplicate)
    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }

  test("test dedup a keepempty=true") {
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(
      plan(pplParser, "source=table | dedup a keepempty=true | fields a", false),
      context)

    val projectList: Seq[NamedExpression] = Seq(UnresolvedAttribute("a"))
    val isNotNullFilter =
      Filter(IsNotNull(UnresolvedAttribute("a")), UnresolvedRelation(Seq("table")))
    val deduplicate = Deduplicate(Seq(UnresolvedAttribute("a")), isNotNullFilter)
    val isNullFilter = Filter(IsNull(UnresolvedAttribute("a")), UnresolvedRelation(Seq("table")))
    val union = Union(deduplicate, isNullFilter)
    val expectedPlan = Project(projectList, union)
    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }

  test("test dedup a, b, c keepempty=true") {
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(
      plan(pplParser, "source=table | dedup a, b, c keepempty=true | fields a, b, c", false),
      context)

    val projectList: Seq[NamedExpression] =
      Seq(UnresolvedAttribute("a"), UnresolvedAttribute("b"), UnresolvedAttribute("c"))
    val isNotNullFilter = Filter(
      And(
        And(IsNotNull(UnresolvedAttribute("a")), IsNotNull(UnresolvedAttribute("b"))),
        IsNotNull(UnresolvedAttribute("c"))),
      UnresolvedRelation(Seq("table")))
    val deduplicate = Deduplicate(
      Seq(UnresolvedAttribute("a"), UnresolvedAttribute("b"), UnresolvedAttribute("c")),
      isNotNullFilter)
    val isNullFilter = Filter(
      Or(
        Or(IsNull(UnresolvedAttribute("a")), IsNull(UnresolvedAttribute("b"))),
        IsNull(UnresolvedAttribute("c"))),
      UnresolvedRelation(Seq("table")))
    val union = Union(deduplicate, isNullFilter)
    val expectedPlan = Project(projectList, union)
    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }

  test("test dedup a consecutive=true") {
    val context = new CatalystPlanContext
    val ex = intercept[UnsupportedOperationException] {
      planTransformer.visit(
        plan(pplParser, "source=table | dedup a consecutive=true | fields a", false),
        context)
    }
    assert(ex.getMessage === "Consecutive deduplication is not supported")
  }

  test("test dedup a keepempty=true consecutive=true") {
    val context = new CatalystPlanContext
    val ex = intercept[UnsupportedOperationException] {
      planTransformer.visit(
        plan(
          pplParser,
          "source=table | dedup a keepempty=true consecutive=true | fields a",
          false),
        context)
    }
    assert(ex.getMessage === "Consecutive deduplication is not supported")
  }

  test("test dedup 1 a") {
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(
      plan(pplParser, "source=table | dedup 1 a | fields a", false),
      context)

    val projectList: Seq[NamedExpression] = Seq(UnresolvedAttribute("a"))
    val filter = Filter(IsNotNull(UnresolvedAttribute("a")), UnresolvedRelation(Seq("table")))
    val deduplicate = Deduplicate(Seq(UnresolvedAttribute("a")), filter)
    val expectedPlan = Project(projectList, deduplicate)
    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }

  test("test dedup 1 a, b, c") {
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(
      plan(pplParser, "source=table | dedup 1 a, b, c | fields a, b, c", false),
      context)

    val projectList: Seq[NamedExpression] =
      Seq(UnresolvedAttribute("a"), UnresolvedAttribute("b"), UnresolvedAttribute("c"))
    val filter = Filter(
      And(
        And(IsNotNull(UnresolvedAttribute("a")), IsNotNull(UnresolvedAttribute("b"))),
        IsNotNull(UnresolvedAttribute("c"))),
      UnresolvedRelation(Seq("table")))
    val deduplicate = Deduplicate(
      Seq(UnresolvedAttribute("a"), UnresolvedAttribute("b"), UnresolvedAttribute("c")),
      filter)
    val expectedPlan = Project(projectList, deduplicate)
    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }

  test("test dedup 1 a keepempty=true") {
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(
      plan(pplParser, "source=table | dedup 1 a keepempty=true | fields a", false),
      context)

    val projectList: Seq[NamedExpression] = Seq(UnresolvedAttribute("a"))
    val isNotNullFilter =
      Filter(IsNotNull(UnresolvedAttribute("a")), UnresolvedRelation(Seq("table")))
    val deduplicate = Deduplicate(Seq(UnresolvedAttribute("a")), isNotNullFilter)
    val isNullFilter = Filter(IsNull(UnresolvedAttribute("a")), UnresolvedRelation(Seq("table")))
    val union = Union(deduplicate, isNullFilter)
    val expectedPlan = Project(projectList, union)
    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }

  test("test dedup 1 a, b, c keepempty=true") {
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(
      plan(pplParser, "source=table | dedup 1 a, b, c keepempty=true | fields a, b, c", false),
      context)

    val projectList: Seq[NamedExpression] =
      Seq(UnresolvedAttribute("a"), UnresolvedAttribute("b"), UnresolvedAttribute("c"))
    val isNotNullFilter = Filter(
      And(
        And(IsNotNull(UnresolvedAttribute("a")), IsNotNull(UnresolvedAttribute("b"))),
        IsNotNull(UnresolvedAttribute("c"))),
      UnresolvedRelation(Seq("table")))
    val deduplicate = Deduplicate(
      Seq(UnresolvedAttribute("a"), UnresolvedAttribute("b"), UnresolvedAttribute("c")),
      isNotNullFilter)
    val isNullFilter = Filter(
      Or(
        Or(IsNull(UnresolvedAttribute("a")), IsNull(UnresolvedAttribute("b"))),
        IsNull(UnresolvedAttribute("c"))),
      UnresolvedRelation(Seq("table")))
    val union = Union(deduplicate, isNullFilter)
    val expectedPlan = Project(projectList, union)
    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }

  test("test dedup 1 a consecutive=true") {
    val context = new CatalystPlanContext
    val ex = intercept[UnsupportedOperationException] {
      planTransformer.visit(
        plan(pplParser, "source=table | dedup 1 a consecutive=true | fields a", false),
        context)
    }
    assert(ex.getMessage === "Consecutive deduplication is not supported")
  }

  test("test dedup 1 a keepempty=true consecutive=true") {
    val context = new CatalystPlanContext
    val ex = intercept[UnsupportedOperationException] {
      planTransformer.visit(
        plan(
          pplParser,
          "source=table | dedup 1 a keepempty=true consecutive=true | fields a",
          false),
        context)
    }
    assert(ex.getMessage === "Consecutive deduplication is not supported")
  }

  test("test dedup 0") {
    val context = new CatalystPlanContext
    val ex = intercept[IllegalArgumentException] {
      planTransformer.visit(
        plan(pplParser, "source=table | dedup 0 a | fields a", false),
        context)
    }
    assert(ex.getMessage === "Number of duplicate events must be greater than 0")
  }

  test("test dedup 2 a") {
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(
      plan(pplParser, "source=table | dedup 2 a | fields a", false),
      context)

    val projectList: Seq[NamedExpression] = Seq(UnresolvedAttribute("a"))
    val isNotNullFilter =
      Filter(IsNotNull(UnresolvedAttribute("a")), UnresolvedRelation(Seq("table")))
    val windowExpression = WindowExpression(
      RowNumber(),
      WindowSpecDefinition(
        UnresolvedAttribute("a") :: Nil,
        SortOrder(UnresolvedAttribute("a"), Ascending) :: Nil,
        SpecifiedWindowFrame(RowFrame, UnboundedPreceding, CurrentRow)))
    val rowNumberAlias = Alias(windowExpression, WindowSpecTransformer.ROW_NUMBER_COLUMN_NAME)()
    val partitionSpec = projectList
    val orderSpec = Seq(SortUtils.sortOrder(UnresolvedAttribute("a"), true))
    val window = Window(Seq(rowNumberAlias), partitionSpec, orderSpec, isNotNullFilter)
    val deduplicateFilter =
      Filter(
        LessThanOrEqual(
          UnresolvedAttribute(WindowSpecTransformer.ROW_NUMBER_COLUMN_NAME),
          Literal(2)),
        window)
    val dropColumns =
      DataFrameDropColumns(
        Seq(UnresolvedAttribute(WindowSpecTransformer.ROW_NUMBER_COLUMN_NAME)),
        deduplicateFilter)
    val expectedPlan = Project(projectList, dropColumns)
    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }

  test("test dedup 2 a, b, c") {
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(
      plan(pplParser, "source=table | dedup 2 a, b, c | fields a, b, c", false),
      context)

    val projectList: Seq[NamedExpression] =
      Seq(UnresolvedAttribute("a"), UnresolvedAttribute("b"), UnresolvedAttribute("c"))
    val isNotNullFilter = Filter(
      And(
        And(IsNotNull(UnresolvedAttribute("a")), IsNotNull(UnresolvedAttribute("b"))),
        IsNotNull(UnresolvedAttribute("c"))),
      UnresolvedRelation(Seq("table")))
    val windowExpression = WindowExpression(
      RowNumber(),
      WindowSpecDefinition(
        UnresolvedAttribute("a") :: UnresolvedAttribute("b") :: UnresolvedAttribute("c") :: Nil,
        SortOrder(UnresolvedAttribute("a"), Ascending) :: SortOrder(
          UnresolvedAttribute("b"),
          Ascending) :: SortOrder(UnresolvedAttribute("c"), Ascending) :: Nil,
        SpecifiedWindowFrame(RowFrame, UnboundedPreceding, CurrentRow)))
    val rowNumberAlias = Alias(windowExpression, WindowSpecTransformer.ROW_NUMBER_COLUMN_NAME)()
    val partitionSpec = projectList
    val orderSpec = Seq(
      SortUtils.sortOrder(UnresolvedAttribute("a"), true),
      SortUtils.sortOrder(UnresolvedAttribute("b"), true),
      SortUtils.sortOrder(UnresolvedAttribute("c"), true))
    val window = Window(Seq(rowNumberAlias), partitionSpec, orderSpec, isNotNullFilter)
    val deduplicateFilter =
      Filter(
        LessThanOrEqual(
          UnresolvedAttribute(WindowSpecTransformer.ROW_NUMBER_COLUMN_NAME),
          Literal(2)),
        window)
    val dropColumns =
      DataFrameDropColumns(
        Seq(UnresolvedAttribute(WindowSpecTransformer.ROW_NUMBER_COLUMN_NAME)),
        deduplicateFilter)
    val expectedPlan = Project(projectList, dropColumns)
    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }

  test("test dedup 2 a keepempty=true") {
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(
      plan(pplParser, "source=table | dedup 2 a keepempty=true | fields a", false),
      context)

    val projectList: Seq[NamedExpression] = Seq(UnresolvedAttribute("a"))
    val isNotNullFilter =
      Filter(IsNotNull(UnresolvedAttribute("a")), UnresolvedRelation(Seq("table")))
    val windowExpression = WindowExpression(
      RowNumber(),
      WindowSpecDefinition(
        UnresolvedAttribute("a") :: Nil,
        SortOrder(UnresolvedAttribute("a"), Ascending) :: Nil,
        SpecifiedWindowFrame(RowFrame, UnboundedPreceding, CurrentRow)))
    val rowNumberAlias = Alias(windowExpression, WindowSpecTransformer.ROW_NUMBER_COLUMN_NAME)()
    val partitionSpec = projectList
    val orderSpec = Seq(SortUtils.sortOrder(UnresolvedAttribute("a"), true))
    val window = Window(Seq(rowNumberAlias), partitionSpec, orderSpec, isNotNullFilter)
    val deduplicateFilter =
      Filter(
        LessThanOrEqual(
          UnresolvedAttribute(WindowSpecTransformer.ROW_NUMBER_COLUMN_NAME),
          Literal(2)),
        window)
    val dropColumns =
      DataFrameDropColumns(
        Seq(UnresolvedAttribute(WindowSpecTransformer.ROW_NUMBER_COLUMN_NAME)),
        deduplicateFilter)

    val isNullFilter = Filter(IsNull(UnresolvedAttribute("a")), UnresolvedRelation(Seq("table")))
    val union = Union(dropColumns, isNullFilter)
    val expectedPlan = Project(projectList, union)
    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }

  test("test dedup 2 a, b, c keepempty=true") {
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(
      plan(pplParser, "source=table | dedup 2 a, b, c keepempty=true | fields a, b, c", false),
      context)

    val projectList: Seq[NamedExpression] =
      Seq(UnresolvedAttribute("a"), UnresolvedAttribute("b"), UnresolvedAttribute("c"))
    val isNotNullFilter = Filter(
      And(
        And(IsNotNull(UnresolvedAttribute("a")), IsNotNull(UnresolvedAttribute("b"))),
        IsNotNull(UnresolvedAttribute("c"))),
      UnresolvedRelation(Seq("table")))
    val windowExpression = WindowExpression(
      RowNumber(),
      WindowSpecDefinition(
        UnresolvedAttribute("a") :: UnresolvedAttribute("b") :: UnresolvedAttribute("c") :: Nil,
        SortOrder(UnresolvedAttribute("a"), Ascending) :: SortOrder(
          UnresolvedAttribute("b"),
          Ascending) :: SortOrder(UnresolvedAttribute("c"), Ascending) :: Nil,
        SpecifiedWindowFrame(RowFrame, UnboundedPreceding, CurrentRow)))
    val rowNumberAlias = Alias(windowExpression, WindowSpecTransformer.ROW_NUMBER_COLUMN_NAME)()
    val partitionSpec = projectList
    val orderSpec = Seq(
      SortUtils.sortOrder(UnresolvedAttribute("a"), true),
      SortUtils.sortOrder(UnresolvedAttribute("b"), true),
      SortUtils.sortOrder(UnresolvedAttribute("c"), true))
    val window = Window(Seq(rowNumberAlias), partitionSpec, orderSpec, isNotNullFilter)
    val deduplicateFilter =
      Filter(
        LessThanOrEqual(
          UnresolvedAttribute(WindowSpecTransformer.ROW_NUMBER_COLUMN_NAME),
          Literal(2)),
        window)
    val dropColumns =
      DataFrameDropColumns(
        Seq(UnresolvedAttribute(WindowSpecTransformer.ROW_NUMBER_COLUMN_NAME)),
        deduplicateFilter)

    val isNullFilter = Filter(
      Or(
        Or(IsNull(UnresolvedAttribute("a")), IsNull(UnresolvedAttribute("b"))),
        IsNull(UnresolvedAttribute("c"))),
      UnresolvedRelation(Seq("table")))
    val union = Union(dropColumns, isNullFilter)
    val expectedPlan = Project(projectList, union)
    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }

  test("test dedup 2 a consecutive=true") {
    val context = new CatalystPlanContext
    val ex = intercept[UnsupportedOperationException] {
      planTransformer.visit(
        plan(pplParser, "source=table | dedup 2 a consecutive=true | fields a | fields a", false),
        context)
    }
    assert(ex.getMessage === "Consecutive deduplication is not supported")
  }

  test("test dedup 2 a keepempty=true consecutive=true") {
    val context = new CatalystPlanContext
    val ex = intercept[UnsupportedOperationException] {
      planTransformer.visit(
        plan(
          pplParser,
          "source=table | dedup 2 a keepempty=true consecutive=true | fields a",
          false),
        context)
    }
    assert(ex.getMessage === "Consecutive deduplication is not supported")
  }
}
