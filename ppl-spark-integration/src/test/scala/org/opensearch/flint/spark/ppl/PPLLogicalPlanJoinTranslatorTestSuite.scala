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
import org.apache.spark.sql.catalyst.expressions.{Alias, Descending, EqualTo, IsNotNull, Literal, NamedExpression, SortOrder}
import org.apache.spark.sql.catalyst.plans.{Cross, Inner, LeftOuter, PlanTest}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Deduplicate, Filter, Join, JoinHint, Project, Sort, SubqueryAlias}

class PPLLogicalPlanJoinTranslatorTestSuite
    extends SparkFunSuite
    with PlanTest
    with LogicalPlanTestUtils
    with Matchers {

  private val planTransformer = new CatalystQueryPlanVisitor()
  private val pplParser = new PPLSyntaxParser()

  /** Test table and index name */
  private val testTable1 = "spark_catalog.default.flint_ppl_test1"
  private val testTable2 = "spark_catalog.default.flint_ppl_test2"
  private val testTable3 = "spark_catalog.default.flint_ppl_test3"

  test("test two-tables inner join: join condition with aliases") {
    val context = new CatalystPlanContext
    val logPlan = plan(
      pplParser,
      s"""
       | source = $testTable1 | JOIN hint.left = l hint.right = r ON l.id = r.id $testTable2
       | """.stripMargin,
      isExplain = false)
    val logicalPlan = planTransformer.visit(logPlan, context)
    val table1 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test1"))
    val table2 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test2"))
    val leftPlan = SubqueryAlias("l", table1)
    val rightPlan = SubqueryAlias("r", table2)
    val joinCondition = EqualTo(UnresolvedAttribute("l.id"), UnresolvedAttribute("r.id"))
    val joinPlan = Join(leftPlan, rightPlan, Inner, Some(joinCondition), JoinHint.NONE)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), joinPlan)
    comparePlans(expectedPlan, logicalPlan, checkAnalysis = false)
  }

  ignore("test two-tables inner join: join condition with table names") {
    val context = new CatalystPlanContext
    val logPlan = plan(
      pplParser,
      s"""
         | source = $testTable1 | JOIN hint.left = l hint.right = r ON $testTable1.id = $testTable2.id $testTable2
         | """.stripMargin,
      isExplain = false)
    val logicalPlan = planTransformer.visit(logPlan, context)
    val table1 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test1"))
    val table2 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test2"))
    val leftPlan = SubqueryAlias("l", table1)
    val rightPlan = SubqueryAlias("r", table2)
    val joinCondition =
      EqualTo(UnresolvedAttribute(s"$testTable1.id"), UnresolvedAttribute(s"$testTable2.id"))
    val joinPlan = Join(leftPlan, rightPlan, Inner, Some(joinCondition), JoinHint.NONE)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), joinPlan)
    comparePlans(expectedPlan, logicalPlan, checkAnalysis = false)
  }

  test("test two-tables inner join: join condition without prefix") {
    val context = new CatalystPlanContext
    val logPlan = plan(
      pplParser,
      s"""
         | source = $testTable1 | JOIN hint.left = l hint.right = r ON id = name $testTable2
         | """.stripMargin,
      isExplain = false)
    val logicalPlan = planTransformer.visit(logPlan, context)
    val table1 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test1"))
    val table2 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test2"))
    val leftPlan = SubqueryAlias("l", table1)
    val rightPlan = SubqueryAlias("r", table2)
    val joinCondition =
      EqualTo(UnresolvedAttribute("id"), UnresolvedAttribute("name"))
    val joinPlan = Join(leftPlan, rightPlan, Inner, Some(joinCondition), JoinHint.NONE)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), joinPlan)
    comparePlans(expectedPlan, logicalPlan, checkAnalysis = false)
  }

  test("test two-tables left outer join") {
    val context = new CatalystPlanContext
    val logPlan = plan(
      pplParser,
      s"""
         | source = $testTable1 | left outer JOIN hint.left = l hint.right = r ON l.id = r.id $testTable2
         | """.stripMargin,
      isExplain = false)
    val logicalPlan = planTransformer.visit(logPlan, context)
    val table1 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test1"))
    val table2 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test2"))
    val leftPlan = SubqueryAlias("l", table1)
    val rightPlan = SubqueryAlias("r", table2)
    val joinCondition = EqualTo(UnresolvedAttribute("l.id"), UnresolvedAttribute("r.id"))
    val joinPlan = Join(leftPlan, rightPlan, LeftOuter, Some(joinCondition), JoinHint.NONE)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), joinPlan)
    comparePlans(expectedPlan, logicalPlan, checkAnalysis = false)
  }

  test("test two-tables cross join") {
    val context = new CatalystPlanContext
    val logPlan = plan(
      pplParser,
      s"""
         | source = $testTable1 | cross JOIN hint.left = l hint.right = r ON l.id = r.id $testTable2
         | """.stripMargin,
      isExplain = false)
    val logicalPlan = planTransformer.visit(logPlan, context)
    val table1 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test1"))
    val table2 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test2"))
    val leftPlan = SubqueryAlias("l", table1)
    val rightPlan = SubqueryAlias("r", table2)
    val joinCondition = EqualTo(UnresolvedAttribute("l.id"), UnresolvedAttribute("r.id"))
    val joinPlan = Join(leftPlan, rightPlan, Cross, Some(joinCondition), JoinHint.NONE)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), joinPlan)
    comparePlans(expectedPlan, logicalPlan, checkAnalysis = false)
  }

  test("test three-tables inner join") {
    val context = new CatalystPlanContext
    val logPlan = plan(
      pplParser,
      s"""
         | source = $testTable1
         | | inner JOIN hint.left = l,hint.right = r ON l.id = r.id $testTable2
         | | left JOIN hint.left = l,hint.right = r ON l.name = r.name $testTable3
         | """.stripMargin,
      isExplain = false)
    val logicalPlan = planTransformer.visit(logPlan, context)
    val table1 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test1"))
    val table2 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test2"))
    val table3 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test3"))
    var leftPlan = SubqueryAlias("l", table1)
    var rightPlan = SubqueryAlias("r", table2)
    val joinCondition1 = EqualTo(UnresolvedAttribute("l.id"), UnresolvedAttribute("r.id"))
    val joinPlan1 = Join(leftPlan, rightPlan, Inner, Some(joinCondition1), JoinHint.NONE)
    leftPlan = SubqueryAlias("l", joinPlan1)
    rightPlan = SubqueryAlias("r", table3)
    val joinCondition2 = EqualTo(UnresolvedAttribute("l.name"), UnresolvedAttribute("r.name"))
    val joinPlan2 = Join(leftPlan, rightPlan, LeftOuter, Some(joinCondition2), JoinHint.NONE)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), joinPlan2)
    comparePlans(expectedPlan, logicalPlan, checkAnalysis = false)
  }

  test("test complex join: TPC-H Q13") {
    val context = new CatalystPlanContext
    val logPlan = plan(
      pplParser,
      s"""
         | SEARCH source = $testTable1
         | | FIELDS id, name
         | | LEFT OUTER JOIN hint.left = c hint.right = o ON c.custkey = o.custkey $testTable2
         | | STATS count(o.orderkey) AS o_count BY c.custkey
         | | STATS count(1) AS custdist BY o_count
         | | SORT - custdist, - o_count
         | """.stripMargin,
      isExplain = false)
    val logicalPlan = planTransformer.visit(logPlan, context)
    val tableC = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test1"))
    val tableO = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test2"))
    val left = SubqueryAlias(
      "c",
      Project(Seq(UnresolvedAttribute("id"), UnresolvedAttribute("name")), tableC))
    val right = SubqueryAlias("o", tableO)
    val joinCondition =
      EqualTo(UnresolvedAttribute("o.custkey"), UnresolvedAttribute("c.custkey"))
    val join = Join(left, right, LeftOuter, Some(joinCondition), JoinHint.NONE)
    val groupingExpression1 = Alias(UnresolvedAttribute("c.custkey"), "c.custkey")()
    val aggregateExpressions1 =
      Alias(
        UnresolvedFunction(
          Seq("COUNT"),
          Seq(UnresolvedAttribute("o.orderkey")),
          isDistinct = false),
        "o_count")()
    val agg1 =
      Aggregate(Seq(groupingExpression1), Seq(aggregateExpressions1, groupingExpression1), join)
    val groupingExpression2 = Alias(UnresolvedAttribute("o_count"), "o_count")()
    val aggregateExpressions2 =
      Alias(UnresolvedFunction(Seq("COUNT"), Seq(Literal(1)), isDistinct = false), "custdist")()
    val agg2 =
      Aggregate(Seq(groupingExpression2), Seq(aggregateExpressions2, groupingExpression2), agg1)
    val sort = Sort(
      Seq(
        SortOrder(UnresolvedAttribute("custdist"), Descending),
        SortOrder(UnresolvedAttribute("o_count"), Descending)),
      global = true,
      agg2)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), sort)
    comparePlans(expectedPlan, logicalPlan, checkAnalysis = false)
  }
}
