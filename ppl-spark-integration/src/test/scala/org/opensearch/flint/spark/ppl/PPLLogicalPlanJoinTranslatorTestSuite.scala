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
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Ascending, Descending, EqualTo, GreaterThan, LessThan, Literal, Not, SortOrder}
import org.apache.spark.sql.catalyst.plans.{Cross, FullOuter, Inner, LeftAnti, LeftOuter, LeftSemi, PlanTest, RightOuter}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, GlobalLimit, Join, JoinHint, LocalLimit, Project, Sort, SubqueryAlias}

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
  private val testTable4 = "spark_catalog.default.flint_ppl_test4"

  test("test two-tables inner join: join condition with aliases") {
    val context = new CatalystPlanContext
    val logPlan = plan(
      pplParser,
      s"""
         | source = $testTable1| JOIN left = l right = r ON l.id = r.id $testTable2
         | """.stripMargin)
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

  test("test two-tables inner join: join condition with table names") {
    val context = new CatalystPlanContext
    val logPlan = plan(
      pplParser,
      s"""
         | source = $testTable1| JOIN left = l right = r ON $testTable1.id = $testTable2.id $testTable2
         | """.stripMargin)
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

  test("test inner join: join condition without prefix") {
    val context = new CatalystPlanContext
    val logPlan = plan(
      pplParser,
      s"""
         | source = $testTable1| JOIN left = l right = r ON id = name $testTable2
         | """.stripMargin)
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

  test("test inner join: join condition with aliases and predicates") {
    val context = new CatalystPlanContext
    val logPlan = plan(
      pplParser,
      s"""
         | source = $testTable1| JOIN left = l right = r ON l.id = r.id AND l.count > 10 AND lower(r.name) = 'hello' $testTable2
         | """.stripMargin)
    val logicalPlan = planTransformer.visit(logPlan, context)
    val table1 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test1"))
    val table2 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test2"))
    val leftPlan = SubqueryAlias("l", table1)
    val rightPlan = SubqueryAlias("r", table2)
    val joinCondition = And(
      And(
        EqualTo(UnresolvedAttribute("l.id"), UnresolvedAttribute("r.id")),
        EqualTo(
          Literal("hello"),
          UnresolvedFunction.apply(
            "lower",
            Seq(UnresolvedAttribute("r.name")),
            isDistinct = false))),
      LessThan(Literal(10), UnresolvedAttribute("l.count")))
    val joinPlan = Join(leftPlan, rightPlan, Inner, Some(joinCondition), JoinHint.NONE)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), joinPlan)
    comparePlans(expectedPlan, logicalPlan, checkAnalysis = false)
  }

  test("test inner join: join condition with table names and predicates") {
    val context = new CatalystPlanContext
    val logPlan = plan(
      pplParser,
      s"""
         | source = $testTable1| INNER JOIN left = l right = r ON $testTable1.id = $testTable2.id AND $testTable1.count > 10 AND lower($testTable2.name) = 'hello' $testTable2
         | """.stripMargin)
    val logicalPlan = planTransformer.visit(logPlan, context)
    val table1 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test1"))
    val table2 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test2"))
    val leftPlan = SubqueryAlias("l", table1)
    val rightPlan = SubqueryAlias("r", table2)
    val joinCondition = And(
      And(
        EqualTo(UnresolvedAttribute(s"$testTable1.id"), UnresolvedAttribute(s"$testTable2.id")),
        EqualTo(
          Literal("hello"),
          UnresolvedFunction.apply(
            "lower",
            Seq(UnresolvedAttribute(s"$testTable2.name")),
            isDistinct = false))),
      LessThan(Literal(10), UnresolvedAttribute(s"$testTable1.count")))
    val joinPlan = Join(leftPlan, rightPlan, Inner, Some(joinCondition), JoinHint.NONE)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), joinPlan)
    comparePlans(expectedPlan, logicalPlan, checkAnalysis = false)
  }

  test("test left outer join") {
    val context = new CatalystPlanContext
    val logPlan = plan(
      pplParser,
      s"""
         | source = $testTable1| LEFT OUTER JOIN left = l right = r ON l.id = r.id $testTable2
         | """.stripMargin)
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

  test("test right outer join") {
    val context = new CatalystPlanContext
    val logPlan = plan(
      pplParser,
      s"""
         | source = $testTable1| RIGHT JOIN left = l right = r ON l.id = r.id $testTable2
         | """.stripMargin)
    val logicalPlan = planTransformer.visit(logPlan, context)
    val table1 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test1"))
    val table2 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test2"))
    val leftPlan = SubqueryAlias("l", table1)
    val rightPlan = SubqueryAlias("r", table2)
    val joinCondition = EqualTo(UnresolvedAttribute("l.id"), UnresolvedAttribute("r.id"))
    val joinPlan = Join(leftPlan, rightPlan, RightOuter, Some(joinCondition), JoinHint.NONE)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), joinPlan)
    comparePlans(expectedPlan, logicalPlan, checkAnalysis = false)
  }

  test("test left semi join") {
    val context = new CatalystPlanContext
    val logPlan = plan(
      pplParser,
      s"""
         | source = $testTable1| LEFT SEMI JOIN left = l right = r ON l.id = r.id $testTable2
         | """.stripMargin)
    val logicalPlan = planTransformer.visit(logPlan, context)
    val table1 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test1"))
    val table2 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test2"))
    val leftPlan = SubqueryAlias("l", table1)
    val rightPlan = SubqueryAlias("r", table2)
    val joinCondition = EqualTo(UnresolvedAttribute("l.id"), UnresolvedAttribute("r.id"))
    val joinPlan = Join(leftPlan, rightPlan, LeftSemi, Some(joinCondition), JoinHint.NONE)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), joinPlan)
    comparePlans(expectedPlan, logicalPlan, checkAnalysis = false)
  }

  test("test left anti join") {
    val context = new CatalystPlanContext
    val logPlan = plan(
      pplParser,
      s"""
         | source = $testTable1| LEFT ANTI JOIN left = l right = r ON l.id = r.id $testTable2
         | """.stripMargin)
    val logicalPlan = planTransformer.visit(logPlan, context)
    val table1 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test1"))
    val table2 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test2"))
    val leftPlan = SubqueryAlias("l", table1)
    val rightPlan = SubqueryAlias("r", table2)
    val joinCondition = EqualTo(UnresolvedAttribute("l.id"), UnresolvedAttribute("r.id"))
    val joinPlan = Join(leftPlan, rightPlan, LeftAnti, Some(joinCondition), JoinHint.NONE)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), joinPlan)
    comparePlans(expectedPlan, logicalPlan, checkAnalysis = false)
  }

  test("test full outer join") {
    val context = new CatalystPlanContext
    val logPlan = plan(
      pplParser,
      s"""
         | source = $testTable1| FULL JOIN left = l right = r ON l.id = r.id $testTable2
         | """.stripMargin)
    val logicalPlan = planTransformer.visit(logPlan, context)
    val table1 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test1"))
    val table2 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test2"))
    val leftPlan = SubqueryAlias("l", table1)
    val rightPlan = SubqueryAlias("r", table2)
    val joinCondition = EqualTo(UnresolvedAttribute("l.id"), UnresolvedAttribute("r.id"))
    val joinPlan = Join(leftPlan, rightPlan, FullOuter, Some(joinCondition), JoinHint.NONE)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), joinPlan)
    comparePlans(expectedPlan, logicalPlan, checkAnalysis = false)
  }

  test("test cross join") {
    val context = new CatalystPlanContext
    val logPlan = plan(
      pplParser,
      s"""
         | source = $testTable1| CROSS JOIN left = l right = r $testTable2
         | """.stripMargin)
    val logicalPlan = planTransformer.visit(logPlan, context)
    val table1 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test1"))
    val table2 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test2"))
    val leftPlan = SubqueryAlias("l", table1)
    val rightPlan = SubqueryAlias("r", table2)
    val joinPlan = Join(leftPlan, rightPlan, Cross, None, JoinHint.NONE)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), joinPlan)
    comparePlans(expectedPlan, logicalPlan, checkAnalysis = false)
  }

  test("test cross join with join condition") {
    val context = new CatalystPlanContext
    val logPlan = plan(
      pplParser,
      s"""
         | source = $testTable1| CROSS JOIN left = l right = r ON l.id = r.id $testTable2
         | """.stripMargin)
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

  test("test multiple joins") {
    val context = new CatalystPlanContext
    val logPlan = plan(
      pplParser,
      s"""
         | source = $testTable1
         | | inner JOIN left = l right = r ON l.id = r.id $testTable2
         | | left JOIN left = l right = r ON l.name = r.name $testTable3
         | | cross JOIN left = l right = r $testTable4
         | """.stripMargin)
    val logicalPlan = planTransformer.visit(logPlan, context)
    val table1 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test1"))
    val table2 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test2"))
    val table3 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test3"))
    val table4 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test4"))
    var leftPlan = SubqueryAlias("l", table1)
    var rightPlan = SubqueryAlias("r", table2)
    val joinCondition1 = EqualTo(UnresolvedAttribute("l.id"), UnresolvedAttribute("r.id"))
    val joinPlan1 = Join(leftPlan, rightPlan, Inner, Some(joinCondition1), JoinHint.NONE)
    leftPlan = SubqueryAlias("l", joinPlan1)
    rightPlan = SubqueryAlias("r", table3)
    val joinCondition2 = EqualTo(UnresolvedAttribute("l.name"), UnresolvedAttribute("r.name"))
    val joinPlan2 = Join(leftPlan, rightPlan, LeftOuter, Some(joinCondition2), JoinHint.NONE)
    leftPlan = SubqueryAlias("l", joinPlan2)
    rightPlan = SubqueryAlias("r", table4)
    val joinPlan3 = Join(leftPlan, rightPlan, Cross, None, JoinHint.NONE)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), joinPlan3)
    comparePlans(expectedPlan, logicalPlan, checkAnalysis = false)
  }

  test("test complex join: TPC-H Q13") {
    val context = new CatalystPlanContext
    val logPlan = plan(
      pplParser,
      s"""
         | SEARCH source = $testTable1
         | | FIELDS id, name
         | | LEFT OUTER JOIN left = c right = o ON c.custkey = o.custkey $testTable2
         | | STATS count(o.orderkey) AS o_count BY c.custkey
         | | STATS count(1) AS custdist BY o_count
         | | SORT - custdist, - o_count
         | """.stripMargin)
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

  test("test inner join with relation subquery") {
    val context = new CatalystPlanContext
    val logPlan = plan(
      pplParser,
      s"""
         | source = $testTable1| JOIN left = l right = r ON l.id = r.id
         |   [
         |     source = $testTable2
         |     | where id > 10 and name = 'abc'
         |     | fields id, name
         |     | sort id
         |     | head 10
         |   ]
         | | stats count(id) as cnt by type
         | """.stripMargin)
    val logicalPlan = planTransformer.visit(logPlan, context)
    val table1 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test1"))
    val table2 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test2"))
    val leftPlan = SubqueryAlias("l", table1)
    val rightSubquery =
      GlobalLimit(
        Literal(10),
        LocalLimit(
          Literal(10),
          Sort(
            Seq(SortOrder(UnresolvedAttribute("id"), Ascending)),
            global = true,
            Project(
              Seq(UnresolvedAttribute("id"), UnresolvedAttribute("name")),
              Filter(
                And(
                  GreaterThan(UnresolvedAttribute("id"), Literal(10)),
                  EqualTo(UnresolvedAttribute("name"), Literal("abc"))),
                table2)))))
    val rightPlan = SubqueryAlias("r", rightSubquery)
    val joinCondition = EqualTo(UnresolvedAttribute("l.id"), UnresolvedAttribute("r.id"))
    val joinPlan = Join(leftPlan, rightPlan, Inner, Some(joinCondition), JoinHint.NONE)
    val groupingExpression = Alias(UnresolvedAttribute("type"), "type")()
    val aggregateExpression = Alias(
      UnresolvedFunction(Seq("COUNT"), Seq(UnresolvedAttribute("id")), isDistinct = false),
      "cnt")()
    val aggPlan =
      Aggregate(Seq(groupingExpression), Seq(aggregateExpression, groupingExpression), joinPlan)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), aggPlan)
    comparePlans(expectedPlan, logicalPlan, checkAnalysis = false)
  }

  test("test left outer join with relation subquery") {
    val context = new CatalystPlanContext
    val logPlan = plan(
      pplParser,
      s"""
         | source = $testTable1| LEFT JOIN left = l right = r ON l.id = r.id
         |   [
         |     source = $testTable2
         |     | where id > 10 and name = 'abc'
         |     | fields id, name
         |     | sort id
         |     | head 10
         |   ]
         | | stats count(id) as cnt by type
         | """.stripMargin)
    val logicalPlan = planTransformer.visit(logPlan, context)
    val table1 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test1"))
    val table2 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test2"))
    val leftPlan = SubqueryAlias("l", table1)
    val rightSubquery =
      GlobalLimit(
        Literal(10),
        LocalLimit(
          Literal(10),
          Sort(
            Seq(SortOrder(UnresolvedAttribute("id"), Ascending)),
            global = true,
            Project(
              Seq(UnresolvedAttribute("id"), UnresolvedAttribute("name")),
              Filter(
                And(
                  GreaterThan(UnresolvedAttribute("id"), Literal(10)),
                  EqualTo(UnresolvedAttribute("name"), Literal("abc"))),
                table2)))))
    val rightPlan = SubqueryAlias("r", rightSubquery)
    val joinCondition = EqualTo(UnresolvedAttribute("l.id"), UnresolvedAttribute("r.id"))
    val joinPlan = Join(leftPlan, rightPlan, LeftOuter, Some(joinCondition), JoinHint.NONE)
    val groupingExpression = Alias(UnresolvedAttribute("type"), "type")()
    val aggregateExpression = Alias(
      UnresolvedFunction(Seq("COUNT"), Seq(UnresolvedAttribute("id")), isDistinct = false),
      "cnt")()
    val aggPlan =
      Aggregate(Seq(groupingExpression), Seq(aggregateExpression, groupingExpression), joinPlan)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), aggPlan)
    comparePlans(expectedPlan, logicalPlan, checkAnalysis = false)
  }

  test("test multiple joins with relation subquery") {
    val context = new CatalystPlanContext
    val logPlan = plan(
      pplParser,
      s"""
         | source = $testTable1
         | | head 10
         | | inner JOIN left = l right = r ON l.id = r.id
         |   [
         |     source = $testTable2
         |     | where id > 10
         |   ]
         | | left JOIN left = l right = r ON l.name = r.name
         |   [
         |     source = $testTable3
         |     | fields id
         |   ]
         | | cross JOIN left = l right = r
         |   [
         |     source = $testTable4
         |     | sort id
         |   ]
         | """.stripMargin)
    val logicalPlan = planTransformer.visit(logPlan, context)
    val table1 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test1"))
    val table2 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test2"))
    val table3 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test3"))
    val table4 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test4"))
    var leftPlan = SubqueryAlias("l", GlobalLimit(Literal(10), LocalLimit(Literal(10), table1)))
    var rightPlan =
      SubqueryAlias("r", Filter(GreaterThan(UnresolvedAttribute("id"), Literal(10)), table2))
    val joinCondition1 = EqualTo(UnresolvedAttribute("l.id"), UnresolvedAttribute("r.id"))
    val joinPlan1 = Join(leftPlan, rightPlan, Inner, Some(joinCondition1), JoinHint.NONE)
    leftPlan = SubqueryAlias("l", joinPlan1)
    rightPlan = SubqueryAlias("r", Project(Seq(UnresolvedAttribute("id")), table3))
    val joinCondition2 = EqualTo(UnresolvedAttribute("l.name"), UnresolvedAttribute("r.name"))
    val joinPlan2 = Join(leftPlan, rightPlan, LeftOuter, Some(joinCondition2), JoinHint.NONE)
    leftPlan = SubqueryAlias("l", joinPlan2)
    rightPlan = SubqueryAlias(
      "r",
      Sort(Seq(SortOrder(UnresolvedAttribute("id"), Ascending)), global = true, table4))
    val joinPlan3 = Join(leftPlan, rightPlan, Cross, None, JoinHint.NONE)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), joinPlan3)
    comparePlans(expectedPlan, logicalPlan, checkAnalysis = false)
  }

  test("test complex join: TPC-H Q13 with relation subquery") {
    // select
    //    c_count,
    //    count(*) as custdist
    // from
    //    (
    //        select
    //            c_custkey,
    //            count(o_orderkey) as c_count
    //        from
    //            customer left outer join orders on
    //                c_custkey = o_custkey
    //                and o_comment not like '%special%requests%'
    //        group by
    //            c_custkey
    //    ) as c_orders
    // group by
    //    c_count
    // order by
    //    custdist desc,
    //    c_count desc
    val context = new CatalystPlanContext
    val logPlan = plan(
      pplParser,
      s"""
         | SEARCH source = [
         |   SEARCH source = customer
         |   | LEFT OUTER JOIN left = c right = o ON c_custkey = o_custkey
         |     [
         |       SEARCH source = orders
         |       | WHERE not like(o_comment, '%special%requests%')
         |     ]
         |   | STATS COUNT(o_orderkey) AS c_count BY c_custkey
         | ] AS c_orders
         | | STATS COUNT(o_orderkey) AS c_count BY c_custkey
         | | STATS COUNT(1) AS custdist BY c_count
         | | SORT - custdist, - c_count
         | """.stripMargin)
    val logicalPlan = planTransformer.visit(logPlan, context)
    val tableC = UnresolvedRelation(Seq("customer"))
    val tableO = UnresolvedRelation(Seq("orders"))
    val left = SubqueryAlias("c", tableC)
    val filterNot = Filter(
      Not(
        UnresolvedFunction(
          Seq("like"),
          Seq(UnresolvedAttribute("o_comment"), Literal("%special%requests%")),
          isDistinct = false)),
      tableO)
    val right = SubqueryAlias("o", filterNot)
    val joinCondition =
      EqualTo(UnresolvedAttribute("o_custkey"), UnresolvedAttribute("c_custkey"))
    val join = Join(left, right, LeftOuter, Some(joinCondition), JoinHint.NONE)
    val groupingExpression1 = Alias(UnresolvedAttribute("c_custkey"), "c_custkey")()
    val aggregateExpressions1 =
      Alias(
        UnresolvedFunction(
          Seq("COUNT"),
          Seq(UnresolvedAttribute("o_orderkey")),
          isDistinct = false),
        "c_count")()
    val agg3 =
      Aggregate(Seq(groupingExpression1), Seq(aggregateExpressions1, groupingExpression1), join)
    val subqueryAlias = SubqueryAlias("c_orders", agg3)
    val agg2 =
      Aggregate(
        Seq(groupingExpression1),
        Seq(aggregateExpressions1, groupingExpression1),
        subqueryAlias)
    val groupingExpression2 = Alias(UnresolvedAttribute("c_count"), "c_count")()
    val aggregateExpressions2 =
      Alias(UnresolvedFunction(Seq("COUNT"), Seq(Literal(1)), isDistinct = false), "custdist")()
    val agg1 =
      Aggregate(Seq(groupingExpression2), Seq(aggregateExpressions2, groupingExpression2), agg2)
    val sort = Sort(
      Seq(
        SortOrder(UnresolvedAttribute("custdist"), Descending),
        SortOrder(UnresolvedAttribute("c_count"), Descending)),
      global = true,
      agg1)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), sort)
    comparePlans(expectedPlan, logicalPlan, checkAnalysis = false)
  }

  test("test multiple joins with table alias") {
    val context = new CatalystPlanContext
    val logPlan = plan(
      pplParser,
      s"""
         | source = table1 as t1
         | | JOIN ON t1.id = t2.id
         |   [
         |     source = table2 as t2
         |   ]
         | | JOIN ON t2.id = t3.id
         |   [
         |     source = table3 as t3
         |   ]
         | | JOIN ON t3.id = t4.id
         |   [
         |     source = table4 as t4
         |   ]
         | """.stripMargin)
    val logicalPlan = planTransformer.visit(logPlan, context)
    val table1 = UnresolvedRelation(Seq("table1"))
    val table2 = UnresolvedRelation(Seq("table2"))
    val table3 = UnresolvedRelation(Seq("table3"))
    val table4 = UnresolvedRelation(Seq("table4"))
    val joinPlan1 = Join(
      SubqueryAlias("t1", table1),
      SubqueryAlias("t2", table2),
      Inner,
      Some(EqualTo(UnresolvedAttribute("t1.id"), UnresolvedAttribute("t2.id"))),
      JoinHint.NONE)
    val joinPlan2 = Join(
      joinPlan1,
      SubqueryAlias("t3", table3),
      Inner,
      Some(EqualTo(UnresolvedAttribute("t2.id"), UnresolvedAttribute("t3.id"))),
      JoinHint.NONE)
    val joinPlan3 = Join(
      joinPlan2,
      SubqueryAlias("t4", table4),
      Inner,
      Some(EqualTo(UnresolvedAttribute("t3.id"), UnresolvedAttribute("t4.id"))),
      JoinHint.NONE)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), joinPlan3)
    comparePlans(expectedPlan, logicalPlan, checkAnalysis = false)
  }

  test("test multiple joins with table and subquery alias") {
    val context = new CatalystPlanContext
    val logPlan = plan(
      pplParser,
      s"""
         | source = table1 as t1
         | | JOIN left = l right = r ON t1.id = t2.id
         |   [
         |     source = table2 as t2
         |   ]
         | | JOIN left = l right = r ON t2.id = t3.id
         |   [
         |     source = table3 as t3
         |   ]
         | | JOIN left = l right = r ON t3.id = t4.id
         |   [
         |     source = table4 as t4
         |   ]
         | """.stripMargin)
    val logicalPlan = planTransformer.visit(logPlan, context)
    val table1 = UnresolvedRelation(Seq("table1"))
    val table2 = UnresolvedRelation(Seq("table2"))
    val table3 = UnresolvedRelation(Seq("table3"))
    val table4 = UnresolvedRelation(Seq("table4"))
    val joinPlan1 = Join(
      SubqueryAlias("l", SubqueryAlias("t1", table1)),
      SubqueryAlias("r", SubqueryAlias("t2", table2)),
      Inner,
      Some(EqualTo(UnresolvedAttribute("t1.id"), UnresolvedAttribute("t2.id"))),
      JoinHint.NONE)
    val joinPlan2 = Join(
      SubqueryAlias("l", joinPlan1),
      SubqueryAlias("r", SubqueryAlias("t3", table3)),
      Inner,
      Some(EqualTo(UnresolvedAttribute("t2.id"), UnresolvedAttribute("t3.id"))),
      JoinHint.NONE)
    val joinPlan3 = Join(
      SubqueryAlias("l", joinPlan2),
      SubqueryAlias("r", SubqueryAlias("t4", table4)),
      Inner,
      Some(EqualTo(UnresolvedAttribute("t3.id"), UnresolvedAttribute("t4.id"))),
      JoinHint.NONE)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), joinPlan3)
    comparePlans(expectedPlan, logicalPlan, checkAnalysis = false)
  }

  test("test multiple joins without table aliases") {
    val context = new CatalystPlanContext
    val logPlan = plan(
      pplParser,
      s"""
         | source = table1
         | | JOIN ON table1.id = table2.id table2
         | | JOIN ON table1.id = table3.id table3
         | | JOIN ON table2.id = table4.id table4
         | """.stripMargin)
    val logicalPlan = planTransformer.visit(logPlan, context)
    val table1 = UnresolvedRelation(Seq("table1"))
    val table2 = UnresolvedRelation(Seq("table2"))
    val table3 = UnresolvedRelation(Seq("table3"))
    val table4 = UnresolvedRelation(Seq("table4"))
    val joinPlan1 = Join(
      table1,
      table2,
      Inner,
      Some(EqualTo(UnresolvedAttribute("table1.id"), UnresolvedAttribute("table2.id"))),
      JoinHint.NONE)
    val joinPlan2 = Join(
      joinPlan1,
      table3,
      Inner,
      Some(EqualTo(UnresolvedAttribute("table1.id"), UnresolvedAttribute("table3.id"))),
      JoinHint.NONE)
    val joinPlan3 = Join(
      joinPlan2,
      table4,
      Inner,
      Some(EqualTo(UnresolvedAttribute("table2.id"), UnresolvedAttribute("table4.id"))),
      JoinHint.NONE)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), joinPlan3)
    comparePlans(expectedPlan, logicalPlan, checkAnalysis = false)
  }

  test("test multiple joins with part subquery aliases") {
    val context = new CatalystPlanContext
    val logPlan = plan(
      pplParser,
      s"""
         | source = table1
         | | JOIN left = t1 right = t2 ON t1.name = t2.name table2
         | | JOIN right = t3 ON t1.name = t3.name table3
         | | JOIN right = t4 ON t2.name = t4.name table4
         | """.stripMargin)
    val logicalPlan = planTransformer.visit(logPlan, context)
    val table1 = UnresolvedRelation(Seq("table1"))
    val table2 = UnresolvedRelation(Seq("table2"))
    val table3 = UnresolvedRelation(Seq("table3"))
    val table4 = UnresolvedRelation(Seq("table4"))
    val joinPlan1 = Join(
      SubqueryAlias("t1", table1),
      SubqueryAlias("t2", table2),
      Inner,
      Some(EqualTo(UnresolvedAttribute("t1.name"), UnresolvedAttribute("t2.name"))),
      JoinHint.NONE)
    val joinPlan2 = Join(
      joinPlan1,
      SubqueryAlias("t3", table3),
      Inner,
      Some(EqualTo(UnresolvedAttribute("t1.name"), UnresolvedAttribute("t3.name"))),
      JoinHint.NONE)
    val joinPlan3 = Join(
      joinPlan2,
      SubqueryAlias("t4", table4),
      Inner,
      Some(EqualTo(UnresolvedAttribute("t2.name"), UnresolvedAttribute("t4.name"))),
      JoinHint.NONE)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), joinPlan3)
    comparePlans(expectedPlan, logicalPlan, checkAnalysis = false)
  }

  test("test multiple joins with self join 1") {
    val context = new CatalystPlanContext
    val logPlan = plan(
      pplParser,
      s"""
         | source = $testTable1
         | | JOIN left = t1 right = t2 ON t1.name = t2.name $testTable2
         | | JOIN right = t3 ON t1.name = t3.name $testTable3
         | | JOIN right = t4 ON t1.name = t4.name $testTable1
         | | fields t1.name, t2.name, t3.name, t4.name
         | """.stripMargin)

    val logicalPlan = planTransformer.visit(logPlan, context)
    val table1 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test1"))
    val table2 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test2"))
    val table3 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test3"))
    val joinPlan1 = Join(
      SubqueryAlias("t1", table1),
      SubqueryAlias("t2", table2),
      Inner,
      Some(EqualTo(UnresolvedAttribute("t1.name"), UnresolvedAttribute("t2.name"))),
      JoinHint.NONE)
    val joinPlan2 = Join(
      joinPlan1,
      SubqueryAlias("t3", table3),
      Inner,
      Some(EqualTo(UnresolvedAttribute("t1.name"), UnresolvedAttribute("t3.name"))),
      JoinHint.NONE)
    val joinPlan3 = Join(
      joinPlan2,
      SubqueryAlias("t4", table1),
      Inner,
      Some(EqualTo(UnresolvedAttribute("t1.name"), UnresolvedAttribute("t4.name"))),
      JoinHint.NONE)
    val expectedPlan = Project(
      Seq(
        UnresolvedAttribute("t1.name"),
        UnresolvedAttribute("t2.name"),
        UnresolvedAttribute("t3.name"),
        UnresolvedAttribute("t4.name")),
      joinPlan3)
    comparePlans(expectedPlan, logicalPlan, checkAnalysis = false)
  }

  test("test multiple joins with self join 2") {
    val context = new CatalystPlanContext
    val logPlan = plan(
      pplParser,
      s"""
         | source = $testTable1
         | | JOIN left = t1 right = t2 ON t1.name = t2.name $testTable2
         | | JOIN right = t3 ON t1.name = t3.name $testTable3
         | | JOIN ON t1.name = t4.name
         |   [
         |     source = $testTable1
         |   ] as t4
         | | fields t1.name, t2.name, t3.name, t4.name
         | """.stripMargin)

    val logicalPlan = planTransformer.visit(logPlan, context)
    val table1 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test1"))
    val table2 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test2"))
    val table3 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test3"))
    val joinPlan1 = Join(
      SubqueryAlias("t1", table1),
      SubqueryAlias("t2", table2),
      Inner,
      Some(EqualTo(UnresolvedAttribute("t1.name"), UnresolvedAttribute("t2.name"))),
      JoinHint.NONE)
    val joinPlan2 = Join(
      joinPlan1,
      SubqueryAlias("t3", table3),
      Inner,
      Some(EqualTo(UnresolvedAttribute("t1.name"), UnresolvedAttribute("t3.name"))),
      JoinHint.NONE)
    val joinPlan3 = Join(
      joinPlan2,
      SubqueryAlias("t4", table1),
      Inner,
      Some(EqualTo(UnresolvedAttribute("t1.name"), UnresolvedAttribute("t4.name"))),
      JoinHint.NONE)
    val expectedPlan = Project(
      Seq(
        UnresolvedAttribute("t1.name"),
        UnresolvedAttribute("t2.name"),
        UnresolvedAttribute("t3.name"),
        UnresolvedAttribute("t4.name")),
      joinPlan3)
    comparePlans(expectedPlan, logicalPlan, checkAnalysis = false)
  }

  test("test side alias will override the subquery alias") {
    val context = new CatalystPlanContext
    val logPlan = plan(
      pplParser,
      s"""
         | source = $testTable1
         | | JOIN left = t1 right = t2 ON t1.name = t2.name [ source = $testTable2 as ttt ] as tt
         | | fields t1.name, t2.name
         | """.stripMargin)
    val logicalPlan = planTransformer.visit(logPlan, context)
    val table1 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test1"))
    val table2 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test2"))
    val joinPlan1 = Join(
      SubqueryAlias("t1", table1),
      SubqueryAlias("t2", SubqueryAlias("tt", SubqueryAlias("ttt", table2))),
      Inner,
      Some(EqualTo(UnresolvedAttribute("t1.name"), UnresolvedAttribute("t2.name"))),
      JoinHint.NONE)
    val expectedPlan =
      Project(Seq(UnresolvedAttribute("t1.name"), UnresolvedAttribute("t2.name")), joinPlan1)
    comparePlans(expectedPlan, logicalPlan, checkAnalysis = false)
  }

  test("test multiple joins with table and subquery backticks alias") {
    val originPlan = plan(
      pplParser,
      s"""
         | source = table1 as t1
         | | JOIN left = l right = r ON t1.id = t2.id
         |   [
         |     source = table2 as t2
         |   ]
         | | JOIN left = l right = r ON t2.id = t3.id
         |   [
         |     source = table3 as t3
         |   ]
         | | JOIN left = l right = r ON t3.id = t4.id
         |   [
         |     source = table4 as t4
         |   ]
         | """.stripMargin)
    val expectedPlan = planTransformer.visit(originPlan, new CatalystPlanContext)
    val logPlan = plan(
      pplParser,
      s"""
         | source = table1 as `t1`
         | | JOIN left = `l` right = `r` ON `t1`.`id` = `t2`.`id`
         |   [
         |     source = table2 as `t2`
         |   ]
         | | JOIN left = `l` right = `r` ON `t2`.`id` = `t3`.`id`
         |   [
         |     source = table3 as `t3`
         |   ]
         | | JOIN left = `l` right = `r` ON `t3`.`id` = `t4`.`id`
         |   [
         |     source = table4 as `t4`
         |   ]
         | """.stripMargin)
    val logicalPlan = planTransformer.visit(logPlan, new CatalystPlanContext)
    comparePlans(expectedPlan, logicalPlan, checkAnalysis = false)
  }

  test("test complex backticks subquery alias") {
    val originPlan = plan(
      pplParser,
      s"""
         | source = $testTable1
         | | JOIN left = t1 right = t2 ON t1.name = t2.name [ source = $testTable2 as ttt ] as tt
         | | fields t1.name, t2.name
         | """.stripMargin)
    val expectedPlan = planTransformer.visit(originPlan, new CatalystPlanContext)
    val logPlan = plan(
      pplParser,
      s"""
         | source = $testTable1
         | | JOIN left = `t1` right = `t2` ON `t1`.`name` = `t2`.`name` [ source = $testTable2 as `ttt` ] as `tt`
         | | fields `t1`.`name`, `t2`.`name`
         | """.stripMargin)
    val logicalPlan = planTransformer.visit(logPlan, new CatalystPlanContext)
    comparePlans(expectedPlan, logicalPlan, checkAnalysis = false)
  }
}
