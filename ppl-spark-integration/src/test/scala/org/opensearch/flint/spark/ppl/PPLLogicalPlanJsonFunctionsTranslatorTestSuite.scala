/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import org.opensearch.flint.spark.ppl.PlaneUtils.plan
import org.opensearch.sql.ppl.{CatalystPlanContext, CatalystQueryPlanVisitor}
import org.opensearch.sql.ppl.utils.DataTypeTransformer.seq
import org.scalatest.matchers.should.Matchers

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{Alias, ArrayExists, ArrayFilter, ArrayForAll, EqualTo, GreaterThan, LambdaFunction, Literal, UnresolvedNamedLambdaVariable}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Project}

class PPLLogicalPlanJsonFunctionsTranslatorTestSuite
    extends SparkFunSuite
    with PlanTest
    with LogicalPlanTestUtils
    with Matchers {

  private val planTransformer = new CatalystQueryPlanVisitor()
  private val pplParser = new PPLSyntaxParser()

  test("test json()") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(pplParser, """source=t a = json('[1,2,3,{"f1":1,"f2":[5,6]},4]')"""),
        context)

    val table = UnresolvedRelation(Seq("t"))
    val jsonFunc =
      UnresolvedFunction(
        "get_json_object",
        Seq(Literal("""[1,2,3,{"f1":1,"f2":[5,6]},4]"""), Literal("$")),
        isDistinct = false)
    val filterExpr = EqualTo(UnresolvedAttribute("a"), jsonFunc)
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedStar(None))
    val expectedPlan = Project(projectList, filterPlan)
    comparePlans(expectedPlan, logPlan, false)
  }

  test("test json_object") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(pplParser, """source=t a = json(json_object('key', array(1, 2, 3)))"""),
        context)

    val table = UnresolvedRelation(Seq("t"))
    val jsonFunc =
      UnresolvedFunction(
        "to_json",
        Seq(
          UnresolvedFunction(
            "named_struct",
            Seq(
              Literal("key"),
              UnresolvedFunction(
                "array",
                Seq(Literal(1), Literal(2), Literal(3)),
                isDistinct = false)),
            isDistinct = false)),
        isDistinct = false)
    val filterExpr = EqualTo(UnresolvedAttribute("a"), jsonFunc)
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedStar(None))
    val expectedPlan = Project(projectList, filterPlan)
    comparePlans(expectedPlan, logPlan, false)
  }

  test("test json_array()") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(pplParser, """source=t a = json_array(1, 2, 0, -1, 1.1, -0.11)"""),
        context)

    val table = UnresolvedRelation(Seq("t"))
    val jsonFunc =
      UnresolvedFunction(
        "array",
        Seq(Literal(1), Literal(2), Literal(0), Literal(-1), Literal(1.1), Literal(-0.11)),
        isDistinct = false)
    val filterExpr = EqualTo(UnresolvedAttribute("a"), jsonFunc)
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedStar(None))
    val expectedPlan = Project(projectList, filterPlan)
    comparePlans(expectedPlan, logPlan, false)
  }

  test("test json_object() and json_array()") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(pplParser, """source=t a = json(json_object('key', json_array(1, 2, 3)))"""),
        context)

    val table = UnresolvedRelation(Seq("t"))
    val jsonFunc =
      UnresolvedFunction(
        "to_json",
        Seq(
          UnresolvedFunction(
            "named_struct",
            Seq(
              Literal("key"),
              UnresolvedFunction(
                "array",
                Seq(Literal(1), Literal(2), Literal(3)),
                isDistinct = false)),
            isDistinct = false)),
        isDistinct = false)
    val filterExpr = EqualTo(UnresolvedAttribute("a"), jsonFunc)
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedStar(None))
    val expectedPlan = Project(projectList, filterPlan)
    comparePlans(expectedPlan, logPlan, false)
  }

  test("test json_array_length(jsonString)") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(pplParser, """source=t a = json_array_length('[1,2,3]')"""),
        context)

    val table = UnresolvedRelation(Seq("t"))
    val jsonFunc =
      UnresolvedFunction("json_array_length", Seq(Literal("""[1,2,3]""")), isDistinct = false)
    val filterExpr = EqualTo(UnresolvedAttribute("a"), jsonFunc)
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedStar(None))
    val expectedPlan = Project(projectList, filterPlan)
    comparePlans(expectedPlan, logPlan, false)
  }

  test("test json_array_length(json_array())") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(pplParser, """source=t a = json_array_length(json_array(1,2,3))"""),
        context)

    val table = UnresolvedRelation(Seq("t"))
    val jsonFunc =
      UnresolvedFunction(
        "json_array_length",
        Seq(
          UnresolvedFunction(
            "to_json",
            Seq(
              UnresolvedFunction(
                "array",
                Seq(Literal(1), Literal(2), Literal(3)),
                isDistinct = false)),
            isDistinct = false)),
        isDistinct = false)
    val filterExpr = EqualTo(UnresolvedAttribute("a"), jsonFunc)
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedStar(None))
    val expectedPlan = Project(projectList, filterPlan)
    comparePlans(expectedPlan, logPlan, false)
  }

  test("test json_extract()") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(pplParser, """source=t a = json_extract('{"a":[{"b":1},{"b":2}]}', '$.a[1].b')"""),
        context)

    val table = UnresolvedRelation(Seq("t"))
    val jsonFunc =
      UnresolvedFunction(
        "get_json_object",
        Seq(Literal("""{"a":[{"b":1},{"b":2}]}"""), Literal("""$.a[1].b""")),
        isDistinct = false)
    val filterExpr = EqualTo(UnresolvedAttribute("a"), jsonFunc)
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedStar(None))
    val expectedPlan = Project(projectList, filterPlan)
    comparePlans(expectedPlan, logPlan, false)
  }

  test("test json_keys()") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(pplParser, """source=t a = json_keys('{"f1":"abc","f2":{"f3":"a","f4":"b"}}')"""),
        context)

    val table = UnresolvedRelation(Seq("t"))
    val jsonFunc =
      UnresolvedFunction(
        "json_object_keys",
        Seq(Literal("""{"f1":"abc","f2":{"f3":"a","f4":"b"}}""")),
        isDistinct = false)
    val filterExpr = EqualTo(UnresolvedAttribute("a"), jsonFunc)
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedStar(None))
    val expectedPlan = Project(projectList, filterPlan)
    comparePlans(expectedPlan, logPlan, false)
  }

  test("json_valid()") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(pplParser, """source=t a = json_valid('[1,2,3,{"f1":1,"f2":[5,6]},4]')"""),
        context)

    val table = UnresolvedRelation(Seq("t"))
    val jsonFunc =
      UnresolvedFunction(
        "isnotnull",
        Seq(
          UnresolvedFunction(
            "get_json_object",
            Seq(Literal("""[1,2,3,{"f1":1,"f2":[5,6]},4]"""), Literal("$")),
            isDistinct = false)),
        isDistinct = false)
    val filterExpr = EqualTo(UnresolvedAttribute("a"), jsonFunc)
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedStar(None))
    val expectedPlan = Project(projectList, filterPlan)
    comparePlans(expectedPlan, logPlan, false)
  }

  test("test json_array_all_match()") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(pplParser,
          """source=t | eval a = json_array(1, 2, 3), b = json_array_all_match(a, x -> x > 0)""".stripMargin),
        context)
    val table = UnresolvedRelation(Seq("t"))
    val jsonFunc =
      UnresolvedFunction(
        "array",
        Seq(Literal(1), Literal(2), Literal(3)),
        isDistinct = false)
    val aliasA = Alias(jsonFunc, "a")()
    val lambda = LambdaFunction(GreaterThan(UnresolvedNamedLambdaVariable(seq("x")), Literal(0)), Seq(UnresolvedNamedLambdaVariable(seq("x"))))
    val aliasB = Alias(ArrayForAll(UnresolvedAttribute("a"), lambda), "b")()
    val evalProject = Project(Seq(UnresolvedStar(None), aliasA, aliasB), table)
    val projectList = Seq(UnresolvedStar(None))
    val expectedPlan = Project(projectList, evalProject)
    comparePlans(expectedPlan, logPlan, false)
  }

  test("test json_array_any_match()") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(pplParser,
          """source=t | eval a = json_array(1, 2, 3), b = json_array_any_match(a, x -> x > 0)""".stripMargin),
        context)
    val table = UnresolvedRelation(Seq("t"))
    val jsonFunc =
      UnresolvedFunction(
        "array",
        Seq(Literal(1), Literal(2), Literal(3)),
        isDistinct = false)
    val aliasA = Alias(jsonFunc, "a")()
    val lambda = LambdaFunction(GreaterThan(UnresolvedNamedLambdaVariable(seq("x")), Literal(0)), Seq(UnresolvedNamedLambdaVariable(seq("x"))))
    val aliasB = Alias(ArrayExists(UnresolvedAttribute("a"), lambda), "b")()
    val evalProject = Project(Seq(UnresolvedStar(None), aliasA, aliasB), table)
    val projectList = Seq(UnresolvedStar(None))
    val expectedPlan = Project(projectList, evalProject)
    comparePlans(expectedPlan, logPlan, false)
  }

  test("test json_array_filter()") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(pplParser,
          """source=t | eval a = json_array(1, 2, 3), b = json_array_filter(a, x -> x > 0)""".stripMargin),
        context)
    val table = UnresolvedRelation(Seq("t"))
    val jsonFunc =
      UnresolvedFunction(
        "array",
        Seq(Literal(1), Literal(2), Literal(3)),
        isDistinct = false)
    val aliasA = Alias(jsonFunc, "a")()
    val lambda = LambdaFunction(GreaterThan(UnresolvedNamedLambdaVariable(seq("x")), Literal(0)), Seq(UnresolvedNamedLambdaVariable(seq("x"))))
    val aliasB = Alias(ArrayFilter(UnresolvedAttribute("a"), lambda), "b")()
    val evalProject = Project(Seq(UnresolvedStar(None), aliasA, aliasB), table)
    val projectList = Seq(UnresolvedStar(None))
    val expectedPlan = Project(projectList, evalProject)
    comparePlans(expectedPlan, logPlan, false)
  }
}
