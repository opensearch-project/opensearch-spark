/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import org.opensearch.flint.spark.ppl.PlaneUtils.plan
import org.opensearch.sql.ppl.{CatalystPlanContext, CatalystQueryPlanVisitor}
import org.scalatest.matchers.should.Matchers

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{Ascending, AttributeReference, Descending, GreaterThan, Literal, NamedExpression, SortOrder}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.command.DescribeTableCommand

class PPLLogicalPlanBasicQueriesTranslatorTestSuite
    extends SparkFunSuite
    with PlanTest
    with LogicalPlanTestUtils
    with Matchers {

  private val planTransformer = new CatalystQueryPlanVisitor()
  private val pplParser = new PPLSyntaxParser()

  test("test error describe clause") {
    val context = new CatalystPlanContext
    val thrown = intercept[IllegalArgumentException] {
      planTransformer.visit(plan(pplParser, "describe t.b.c.d", false), context)
    }

    assert(
      thrown.getMessage === "Invalid table name: t.b.c.d Syntax: [ database_name. ] table_name")
  }

  test("test describe FQN table clause") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(plan(pplParser, "describe schema.default.http_logs", false), context)

    val expectedPlan = DescribeTableCommand(
      TableIdentifier("http_logs", Option("schema"), Option("default")),
      Map.empty[String, String].empty,
      isExtended = true,
      output = DescribeRelation.getOutputAttrs)
    comparePlans(expectedPlan, logPlan, false)
  }

  test("test simple describe clause") {
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(plan(pplParser, "describe t", false), context)

    val expectedPlan = DescribeTableCommand(
      TableIdentifier("t"),
      Map.empty[String, String],
      isExtended = true,
      output = DescribeRelation.getOutputAttrs)
    comparePlans(expectedPlan, logPlan, false)
  }

  test("test FQN table describe table clause") {
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(plan(pplParser, "describe catalog.t", false), context)

    val expectedPlan = DescribeTableCommand(
      TableIdentifier("t", Option("catalog")),
      Map.empty[String, String].empty,
      isExtended = true,
      output = DescribeRelation.getOutputAttrs)
    comparePlans(expectedPlan, logPlan, false)
  }

  test("test simple search with only one table and no explicit fields (defaults to all fields)") {
    // if successful build ppl logical plan and translate to catalyst logical plan
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(plan(pplParser, "source=table", false), context)

    val projectList: Seq[NamedExpression] = Seq(UnresolvedStar(None))
    val expectedPlan = Project(projectList, UnresolvedRelation(Seq("table")))
    comparePlans(expectedPlan, logPlan, false)
  }

  test("test simple search with escaped table name") {
    // if successful build ppl logical plan and translate to catalyst logical plan
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(plan(pplParser, "source=`table`", false), context)

    val projectList: Seq[NamedExpression] = Seq(UnresolvedStar(None))
    val expectedPlan = Project(projectList, UnresolvedRelation(Seq("table")))
    comparePlans(expectedPlan, logPlan, false)
  }

  test("test simple search with schema.table and no explicit fields (defaults to all fields)") {
    // if successful build ppl logical plan and translate to catalyst logical plan
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(plan(pplParser, "source=schema.table", false), context)

    val projectList: Seq[NamedExpression] = Seq(UnresolvedStar(None))
    val expectedPlan = Project(projectList, UnresolvedRelation(Seq("schema", "table")))
    comparePlans(expectedPlan, logPlan, false)

  }

  test("test simple search with schema.table and one field projected") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(plan(pplParser, "source=schema.table | fields A", false), context)

    val projectList: Seq[NamedExpression] = Seq(UnresolvedAttribute("A"))
    val expectedPlan = Project(projectList, UnresolvedRelation(Seq("schema", "table")))
    comparePlans(expectedPlan, logPlan, false)
  }

  test("create ppl simple query with nested field 1 range filter test") {
    val context = new CatalystPlanContext
    val logicalPlan =
      planTransformer.visit(
        plan(
          pplParser,
          "source = spark_catalog.default.flint_ppl_test | where struct_col.field2 > 200 | sort  - struct_col.field2 | fields  int_col, struct_col.field2",
          isExplain = false),
        context)

    // Define the expected logical plan
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    // Define the expected logical plan components
    val filterPlan =
      Filter(GreaterThan(UnresolvedAttribute("struct_col.field2"), Literal(200)), table)
    val sortedPlan: LogicalPlan =
      Sort(
        Seq(SortOrder(UnresolvedAttribute("struct_col.field2"), Descending)),
        global = true,
        filterPlan)
    val expectedPlan =
      Project(
        Seq(UnresolvedAttribute("int_col"), UnresolvedAttribute("struct_col.field2")),
        sortedPlan)

    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  test("test simple search with schema.table and one nested field projected") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(pplParser, "source=schema.table | fields A.nested", false),
        context)

    val projectList: Seq[NamedExpression] = Seq(UnresolvedAttribute("A.nested"))
    val expectedPlan = Project(projectList, UnresolvedRelation(Seq("schema", "table")))
    comparePlans(expectedPlan, logPlan, false)
  }

  test("test simple search with only one table with one field projected") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(plan(pplParser, "source=table | fields A", false), context)

    val projectList: Seq[NamedExpression] = Seq(UnresolvedAttribute("A"))
    val expectedPlan = Project(projectList, UnresolvedRelation(Seq("table")))
    comparePlans(expectedPlan, logPlan, false)
  }

  test("test simple search with only one table with two fields projected") {
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(plan(pplParser, "source=t | fields A, B", false), context)

    val table = UnresolvedRelation(Seq("t"))
    val projectList = Seq(UnresolvedAttribute("A"), UnresolvedAttribute("B"))
    val expectedPlan = Project(projectList, table)
    comparePlans(expectedPlan, logPlan, false)
  }

  test("test simple search with one table with two fields projected sorted by one field") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(plan(pplParser, "source=t | sort A | fields A, B", false), context)

    val table = UnresolvedRelation(Seq("t"))
    val projectList = Seq(UnresolvedAttribute("A"), UnresolvedAttribute("B"))
    // Sort by A ascending
    val sortOrder = Seq(SortOrder(UnresolvedAttribute("A"), Ascending))
    val sorted = Sort(sortOrder, true, table)
    val expectedPlan = Project(projectList, sorted)

    comparePlans(expectedPlan, logPlan, false)
  }

  test("test simple search with one table with two fields projected sorted by one nested field") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(pplParser, "source=t | sort A.nested | fields A.nested, B", false),
        context)

    val table = UnresolvedRelation(Seq("t"))
    val projectList = Seq(UnresolvedAttribute("A.nested"), UnresolvedAttribute("B"))
    // Sort by A ascending
    val sortOrder = Seq(SortOrder(UnresolvedAttribute("A.nested"), Ascending))
    val sorted = Sort(sortOrder, true, table)
    val expectedPlan = Project(projectList, sorted)

    comparePlans(expectedPlan, logPlan, false)
  }

  test(
    "test simple search with only one table with two fields with head (limit ) command projected") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(plan(pplParser, "source=t | fields A, B | head 5", false), context)

    val table = UnresolvedRelation(Seq("t"))
    val projectList = Seq(UnresolvedAttribute("A"), UnresolvedAttribute("B"))
    val planWithLimit =
      GlobalLimit(Literal(5), LocalLimit(Literal(5), Project(projectList, table)))
    val expectedPlan = Project(Seq(UnresolvedStar(None)), planWithLimit)
    comparePlans(expectedPlan, logPlan, false)
  }

  test(
    "test simple search with only one table with two fields with head (limit ) command projected  sorted by one descending field") {
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(
      plan(pplParser, "source=t | sort - A | fields A, B | head 5", false),
      context)

    val table = UnresolvedRelation(Seq("t"))
    val sortOrder = Seq(SortOrder(UnresolvedAttribute("A"), Descending))
    val sorted = Sort(sortOrder, true, table)
    val projectList = Seq(UnresolvedAttribute("A"), UnresolvedAttribute("B"))
    val projectAB = Project(projectList, sorted)

    val planWithLimit = GlobalLimit(Literal(5), LocalLimit(Literal(5), projectAB))
    val expectedPlan = Project(Seq(UnresolvedStar(None)), planWithLimit)
    comparePlans(expectedPlan, logPlan, false)
  }

  test(
    "Search multiple tables - translated into union call - fields expected to exist in both tables ") {
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(
      plan(pplParser, "search source = table1, table2 | fields A, B", false),
      context)

    val table1 = UnresolvedRelation(Seq("table1"))
    val table2 = UnresolvedRelation(Seq("table2"))

    val allFields1 = Seq(UnresolvedAttribute("A"), UnresolvedAttribute("B"))
    val allFields2 = Seq(UnresolvedAttribute("A"), UnresolvedAttribute("B"))

    val projectedTable1 = Project(allFields1, table1)
    val projectedTable2 = Project(allFields2, table2)

    val expectedPlan =
      Union(Seq(projectedTable1, projectedTable2), byName = true, allowMissingCol = true)

    comparePlans(expectedPlan, logPlan, false)
  }

  test("Search multiple tables - translated into union call with fields") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(plan(pplParser, "source = table1, table2  ", false), context)

    val table1 = UnresolvedRelation(Seq("table1"))
    val table2 = UnresolvedRelation(Seq("table2"))

    val allFields1 = UnresolvedStar(None)
    val allFields2 = UnresolvedStar(None)

    val projectedTable1 = Project(Seq(allFields1), table1)
    val projectedTable2 = Project(Seq(allFields2), table2)

    val expectedPlan =
      Union(Seq(projectedTable1, projectedTable2), byName = true, allowMissingCol = true)

    comparePlans(expectedPlan, logPlan, false)
  }
}
