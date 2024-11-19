/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import org.opensearch.flint.spark.ppl.PlaneUtils.plan
import org.opensearch.sql.ppl.{CatalystPlanContext, CatalystQueryPlanVisitor}
import org.scalatest.matchers.should.Matchers

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{EqualTo, Literal}
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
        plan(pplParser, """source=t a = to_json_string(json_object('key', array(1, 2, 3)))"""),
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
        plan(
          pplParser,
          """source=t a = to_json_string(json_object('key', json_array(1, 2, 3)))"""),
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

  test("test array_length(json_array())") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(pplParser, """source=t a = array_length(json_array(1,2,3))"""),
        context)

    val table = UnresolvedRelation(Seq("t"))
    val jsonFunc =
      UnresolvedFunction(
        "array_size",
        Seq(
          UnresolvedFunction(
            "array",
            Seq(Literal(1), Literal(2), Literal(3)),
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
}
