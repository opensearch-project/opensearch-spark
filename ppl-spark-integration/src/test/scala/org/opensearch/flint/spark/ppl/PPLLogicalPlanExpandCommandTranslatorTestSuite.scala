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
import org.apache.spark.sql.catalyst.expressions.{Alias, Explode, GeneratorOuter, Literal, RegExpExtract}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, DataFrameDropColumns, Generate, Project}
import org.apache.spark.sql.types.IntegerType

class PPLLogicalPlanExpandCommandTranslatorTestSuite
    extends SparkFunSuite
    with PlanTest
    with LogicalPlanTestUtils
    with Matchers {

  private val planTransformer = new CatalystQueryPlanVisitor()
  private val pplParser = new PPLSyntaxParser()

  test("test expand only field") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(plan(pplParser, "source=relation | expand field_with_array"), context)

    val relation = UnresolvedRelation(Seq("relation"))
    val generator = Explode(UnresolvedAttribute("field_with_array"))
    val generate = Generate(generator, seq(), false, None, seq(), relation)
    val expectedPlan = Project(seq(UnresolvedStar(None)), generate)
    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }

  test("expand multi columns array table") {
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(
      plan(
        pplParser,
        s"""
         | source = table
         | | expand multi_valueA as multiA
         | | expand multi_valueB as multiB
         | """.stripMargin),
      context)

    val relation = UnresolvedRelation(Seq("table"))
    val generatorA = Explode(UnresolvedAttribute("multi_valueA"))
    val generateA =
      Generate(generatorA, seq(), false, None, seq(UnresolvedAttribute("multiA")), relation)
    val dropSourceColumnA =
      DataFrameDropColumns(Seq(UnresolvedAttribute("multi_valueA")), generateA)
    val generatorB = Explode(UnresolvedAttribute("multi_valueB"))
    val generateB = Generate(
      generatorB,
      seq(),
      false,
      None,
      seq(UnresolvedAttribute("multiB")),
      dropSourceColumnA)
    val dropSourceColumnB =
      DataFrameDropColumns(Seq(UnresolvedAttribute("multi_valueB")), generateB)
    val expectedPlan = Project(seq(UnresolvedStar(None)), dropSourceColumnB)
    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }

  test("test expand on array field which is eval array=json_array") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(
          pplParser,
          "source = table | eval array=json_array(1, 2, 3) | expand array as uid | fields uid"),
        context)

    val relation = UnresolvedRelation(Seq("table"))
    val jsonFunc =
      UnresolvedFunction("array", Seq(Literal(1), Literal(2), Literal(3)), isDistinct = false)
    val aliasA = Alias(jsonFunc, "array")()
    val project = Project(seq(UnresolvedStar(None), aliasA), relation)
    val generate = Generate(
      Explode(UnresolvedAttribute("array")),
      seq(),
      false,
      None,
      seq(UnresolvedAttribute("uid")),
      project)
    val dropSourceColumn =
      DataFrameDropColumns(Seq(UnresolvedAttribute("array")), generate)
    val expectedPlan = Project(seq(UnresolvedAttribute("uid")), dropSourceColumn)
    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }

  test("test expand only field with alias") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(pplParser, "source=relation | expand field_with_array as array_list "),
        context)

    val relation = UnresolvedRelation(Seq("relation"))
    val generate = Generate(
      Explode(UnresolvedAttribute("field_with_array")),
      seq(),
      false,
      None,
      seq(UnresolvedAttribute("array_list")),
      relation)
    val dropSourceColumn =
      DataFrameDropColumns(Seq(UnresolvedAttribute("field_with_array")), generate)
    val expectedPlan = Project(seq(UnresolvedStar(None)), dropSourceColumn)
    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }

  test("test expand and stats") {
    val context = new CatalystPlanContext
    val query =
      "source = table | expand employee | stats max(salary) as max by state, company"
    val logPlan =
      planTransformer.visit(plan(pplParser, query), context)
    val table = UnresolvedRelation(Seq("table"))
    val generate =
      Generate(Explode(UnresolvedAttribute("employee")), seq(), false, None, seq(), table)
    val average = Alias(
      UnresolvedFunction(seq("MAX"), seq(UnresolvedAttribute("salary")), false, None, false),
      "max")()
    val state = Alias(UnresolvedAttribute("state"), "state")()
    val company = Alias(UnresolvedAttribute("company"), "company")()
    val groupingState = Alias(UnresolvedAttribute("state"), "state")()
    val groupingCompany = Alias(UnresolvedAttribute("company"), "company")()
    val aggregate =
      Aggregate(Seq(groupingState, groupingCompany), Seq(average, state, company), generate)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), aggregate)

    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }

  test("test expand and stats with alias") {
    val context = new CatalystPlanContext
    val query =
      "source = table | expand employee as workers | stats max(salary) as max by state, company"
    val logPlan =
      planTransformer.visit(plan(pplParser, query), context)
    val table = UnresolvedRelation(Seq("table"))
    val generate = Generate(
      Explode(UnresolvedAttribute("employee")),
      seq(),
      false,
      None,
      seq(UnresolvedAttribute("workers")),
      table)
    val dropSourceColumn = DataFrameDropColumns(Seq(UnresolvedAttribute("employee")), generate)
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
      dropSourceColumn)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), aggregate)

    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }

  test("test expand and eval") {
    val context = new CatalystPlanContext
    val query = "source = table | expand employee | eval bonus = salary * 3"
    val logPlan = planTransformer.visit(plan(pplParser, query), context)
    val table = UnresolvedRelation(Seq("table"))
    val generate =
      Generate(Explode(UnresolvedAttribute("employee")), seq(), false, None, seq(), table)
    val bonusProject = Project(
      Seq(
        UnresolvedStar(None),
        Alias(
          UnresolvedFunction(
            "*",
            Seq(UnresolvedAttribute("salary"), Literal(3, IntegerType)),
            isDistinct = false),
          "bonus")()),
      generate)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), bonusProject)
    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }

  test("test expand and eval with fields and alias") {
    val context = new CatalystPlanContext
    val query =
      "source = table | expand employee as worker | eval bonus = salary * 3 | fields worker, bonus "
    val logPlan = planTransformer.visit(plan(pplParser, query), context)
    val table = UnresolvedRelation(Seq("table"))
    val generate = Generate(
      Explode(UnresolvedAttribute("employee")),
      seq(),
      false,
      None,
      seq(UnresolvedAttribute("worker")),
      table)
    val dropSourceColumn =
      DataFrameDropColumns(Seq(UnresolvedAttribute("employee")), generate)
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
    val expectedPlan =
      Project(Seq(UnresolvedAttribute("worker"), UnresolvedAttribute("bonus")), bonusProject)
    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }

  test("test expand and parse and fields") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(
          pplParser,
          "source=table | expand employee | parse description '(?<email>.+@.+)' | fields employee, email"),
        context)
    val table = UnresolvedRelation(Seq("table"))
    val generator =
      Generate(Explode(UnresolvedAttribute("employee")), seq(), false, None, seq(), table)
    val emailAlias =
      Alias(
        RegExpExtract(UnresolvedAttribute("description"), Literal("(?<email>.+@.+)"), Literal(1)),
        "email")()
    val parseProject = Project(
      Seq(UnresolvedAttribute("description"), emailAlias, UnresolvedStar(None)),
      generator)
    val expectedPlan =
      Project(Seq(UnresolvedAttribute("employee"), UnresolvedAttribute("email")), parseProject)
    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }

  test("test expand and parse and flatten ") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(
          pplParser,
          "source=relation | expand employee | parse description '(?<email>.+@.+)' | flatten roles "),
        context)
    val table = UnresolvedRelation(Seq("relation"))
    val generateEmployee =
      Generate(Explode(UnresolvedAttribute("employee")), seq(), false, None, seq(), table)
    val emailAlias =
      Alias(
        RegExpExtract(UnresolvedAttribute("description"), Literal("(?<email>.+@.+)"), Literal(1)),
        "email")()
    val parseProject = Project(
      Seq(UnresolvedAttribute("description"), emailAlias, UnresolvedStar(None)),
      generateEmployee)
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

}
