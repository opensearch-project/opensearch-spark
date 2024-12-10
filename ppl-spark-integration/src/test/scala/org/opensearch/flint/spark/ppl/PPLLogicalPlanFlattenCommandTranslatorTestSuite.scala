/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import org.opensearch.flint.spark.FlattenGenerator
import org.opensearch.flint.spark.ppl.PlaneUtils.plan
import org.opensearch.sql.ppl.{CatalystPlanContext, CatalystQueryPlanVisitor}
import org.opensearch.sql.ppl.utils.DataTypeTransformer.seq
import org.scalatest.matchers.should.Matchers

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{Alias, GeneratorOuter, Literal, RegExpExtract}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, DataFrameDropColumns, Generate, Project}
import org.apache.spark.sql.types.IntegerType

class PPLLogicalPlanFlattenCommandTranslatorTestSuite
    extends SparkFunSuite
    with PlanTest
    with LogicalPlanTestUtils
    with Matchers {

  private val planTransformer = new CatalystQueryPlanVisitor()
  private val pplParser = new PPLSyntaxParser()

  test("test flatten only field") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(pplParser, "source=relation | flatten field_with_array"),
        context)

    val relation = UnresolvedRelation(Seq("relation"))
    val flattenGenerator = new FlattenGenerator(UnresolvedAttribute("field_with_array"))
    val outerGenerator = GeneratorOuter(flattenGenerator)
    val generate = Generate(outerGenerator, seq(), true, None, seq(), relation)
    val dropSourceColumn =
      DataFrameDropColumns(Seq(UnresolvedAttribute("field_with_array")), generate)
    val expectedPlan = Project(seq(UnresolvedStar(None)), dropSourceColumn)
    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }

  test("test flatten and stats") {
    val context = new CatalystPlanContext
    val query =
      "source = relation | fields state, company, employee | flatten employee | fields state, company, salary  | stats max(salary) as max by state, company"
    val logPlan =
      planTransformer.visit(plan(pplParser, query), context)
    val table = UnresolvedRelation(Seq("relation"))
    val projectStateCompanyEmployee =
      Project(
        Seq(
          UnresolvedAttribute("state"),
          UnresolvedAttribute("company"),
          UnresolvedAttribute("employee")),
        table)
    val generate = Generate(
      GeneratorOuter(new FlattenGenerator(UnresolvedAttribute("employee"))),
      seq(),
      true,
      None,
      seq(),
      projectStateCompanyEmployee)
    val dropSourceColumn = DataFrameDropColumns(Seq(UnresolvedAttribute("employee")), generate)
    val projectStateCompanySalary = Project(
      Seq(
        UnresolvedAttribute("state"),
        UnresolvedAttribute("company"),
        UnresolvedAttribute("salary")),
      dropSourceColumn)
    val average = Alias(
      UnresolvedFunction(seq("MAX"), seq(UnresolvedAttribute("salary")), false, None, false),
      "max")()
    val state = Alias(UnresolvedAttribute("state"), "state")()
    val company = Alias(UnresolvedAttribute("company"), "company")()
    val groupingState = Alias(UnresolvedAttribute("state"), "state")()
    val groupingCompany = Alias(UnresolvedAttribute("company"), "company")()
    val aggregate = Aggregate(
      Seq(groupingState, groupingCompany),
      Seq(average, state, company),
      projectStateCompanySalary)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), aggregate)

    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }

  test("test flatten and eval") {
    val context = new CatalystPlanContext
    val query = "source = relation | flatten employee | eval bonus = salary * 3"
    val logPlan = planTransformer.visit(plan(pplParser, query), context)
    val table = UnresolvedRelation(Seq("relation"))
    val generate = Generate(
      GeneratorOuter(new FlattenGenerator(UnresolvedAttribute("employee"))),
      seq(),
      true,
      None,
      seq(),
      table)
    val dropSourceColumn = DataFrameDropColumns(Seq(UnresolvedAttribute("employee")), generate)
    val bonusProject = Project(
      Seq(
        UnresolvedStar(None),
        Alias(
          UnresolvedFunction(
            "*",
            Seq(UnresolvedAttribute("salary"), Literal(3, IntegerType)),
            isDistinct = false),
          "bonus")()),
      dropSourceColumn)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), bonusProject)
    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }

  test("test flatten and parse and flatten") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(
          pplParser,
          "source=relation | flatten employee | parse description '(?<email>.+@.+)' | flatten roles"),
        context)
    val table = UnresolvedRelation(Seq("relation"))
    val generateEmployee = Generate(
      GeneratorOuter(new FlattenGenerator(UnresolvedAttribute("employee"))),
      seq(),
      true,
      None,
      seq(),
      table)
    val dropSourceColumnEmployee =
      DataFrameDropColumns(Seq(UnresolvedAttribute("employee")), generateEmployee)
    val emailAlias =
      Alias(
        RegExpExtract(UnresolvedAttribute("description"), Literal("(?<email>.+@.+)"), Literal(1)),
        "email")()
    val parseProject = Project(
      Seq(UnresolvedAttribute("description"), emailAlias, UnresolvedStar(None)),
      dropSourceColumnEmployee)
    val generateRoles = Generate(
      GeneratorOuter(new FlattenGenerator(UnresolvedAttribute("roles"))),
      seq(),
      true,
      None,
      seq(),
      parseProject)
    val dropSourceColumnRoles =
      DataFrameDropColumns(Seq(UnresolvedAttribute("roles")), generateRoles)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), dropSourceColumnRoles)
    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }

  test("test flatten with one alias") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(pplParser, "source=relation | flatten field_with_array as col1"),
        context)

    val relation = UnresolvedRelation(Seq("relation"))
    val flattenGenerator = new FlattenGenerator(UnresolvedAttribute("field_with_array"))
    val outerGenerator = GeneratorOuter(flattenGenerator)
    val generate =
      Generate(outerGenerator, seq(), true, None, Seq(UnresolvedAttribute("col1")), relation)
    val dropSourceColumn =
      DataFrameDropColumns(Seq(UnresolvedAttribute("field_with_array")), generate)
    val expectedPlan = Project(seq(UnresolvedStar(None)), dropSourceColumn)
    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }

  test("test flatten with alias list") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(pplParser, "source=relation | flatten field_with_array as (col1, col2)"),
        context)

    val relation = UnresolvedRelation(Seq("relation"))
    val flattenGenerator = new FlattenGenerator(UnresolvedAttribute("field_with_array"))
    val outerGenerator = GeneratorOuter(flattenGenerator)
    val generate = Generate(
      outerGenerator,
      seq(),
      true,
      None,
      Seq(UnresolvedAttribute("col1"), UnresolvedAttribute("col2")),
      relation)
    val dropSourceColumn =
      DataFrameDropColumns(Seq(UnresolvedAttribute("field_with_array")), generate)
    val expectedPlan = Project(seq(UnresolvedStar(None)), dropSourceColumn)
    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }

}
