/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import java.nio.file.Paths

import org.opensearch.flint.spark.ppl.PlaneUtils.plan
import org.opensearch.sql.common.antlr.SyntaxCheckException
import org.opensearch.sql.ppl.{CatalystPlanContext, CatalystQueryPlanVisitor}
import org.scalatest.matchers.should.Matchers

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction, UnresolvedIdentifier, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{And, Ascending, Descending, EqualTo, GreaterThan, LessThan, Literal, NamedExpression, Not, SortOrder}
import org.apache.spark.sql.catalyst.expressions.aggregate.AnyValue
import org.apache.spark.sql.catalyst.plans.{Inner, PlanTest}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.connector.expressions.{Expression, FieldReference, IdentityTransform, NamedReference, Transform}

class PPLLogicalPlanProjectQueriesTranslatorTestSuite
    extends SparkFunSuite
    with PlanTest
    with LogicalPlanTestUtils
    with Matchers {

  private val testTable1 = "spark_catalog.default.flint_ppl_test1"
  private val testTable2 = "spark_catalog.default.flint_ppl_test2"

  private val planTransformer = new CatalystQueryPlanVisitor()
  private val pplParser = new PPLSyntaxParser()
  private val viewFolderLocation = Paths.get(".", "spark-warehouse", "student_partition_bucket")

  test("test project a simple search with only one table using csv ") {
    // if successful build ppl logical plan and translate to catalyst logical plan
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(
      plan(
        pplParser,
        "project simpleView using csv | source = table | where state != 'California' | fields name"),
      context)

    // Define the expected logical plan
    val relation = UnresolvedRelation(Seq("table"))
    val filter =
      Filter(Not(EqualTo(UnresolvedAttribute("state"), Literal("California"))), relation)
    val expectedPlan: LogicalPlan =
      CreateTableAsSelect(
        UnresolvedIdentifier(Seq("simpleView")),
        Seq(),
        Project(Seq(UnresolvedAttribute("name")), filter),
        UnresolvedTableSpec(
          Map.empty,
          Option("CSV"),
          OptionList(Seq()),
          Option.empty,
          Option.empty,
          Option.empty,
          external = false),
        Map.empty,
        ignoreIfExists = false,
        isAnalyzed = false)
    // Compare the two plans
    comparePlans(logPlan, expectedPlan, checkAnalysis = false)
  }

  test("test project a simple search with only one table using csv and partitioned field ") {
    // if successful build ppl logical plan and translate to catalyst logical plan
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(
      plan(
        pplParser,
        "project if not exists simpleView using csv partitioned by (age) | source = table | where state != 'California' "),
      context)

    // Define the expected logical plan
    val relation = UnresolvedRelation(Seq("table"))
    val filter =
      Filter(Not(EqualTo(UnresolvedAttribute("state"), Literal("California"))), relation)
    val expectedPlan: LogicalPlan =
      CreateTableAsSelect(
        UnresolvedIdentifier(Seq("simpleView")),
        Seq(),
//        Seq(IdentityTransform.apply(FieldReference.apply("age"))),
        Project(Seq(UnresolvedStar(None)), filter),
        UnresolvedTableSpec(
          Map.empty,
          Option("CSV"),
          OptionList(Seq()),
          Option.empty,
          Option.empty,
          Option.empty,
          external = false),
        Map.empty,
        ignoreIfExists = true,
        isAnalyzed = false)
    // Compare the two plans
    assert(compareByString(logPlan) == expectedPlan.toString)
  }

  test(
    "test project a simple search with only one table using csv and multiple partitioned fields ") {
    // if successful build ppl logical plan and translate to catalyst logical plan
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(
      plan(
        pplParser,
        "project if not exists simpleView using csv partitioned by (age, country) | source = table | where state != 'California' "),
      context)

    // Define the expected logical plan
    val relation = UnresolvedRelation(Seq("table"))
    val filter =
      Filter(Not(EqualTo(UnresolvedAttribute("state"), Literal("California"))), relation)
    val expectedPlan: LogicalPlan =
      CreateTableAsSelect(
        UnresolvedIdentifier(Seq("simpleView")),
        Seq(),
//        Seq(IdentityTransform.apply(FieldReference.apply("age")), IdentityTransform.apply(FieldReference.apply("country"))),
        Project(Seq(UnresolvedStar(None)), filter),
        UnresolvedTableSpec(
          Map.empty,
          Option("CSV"),
          OptionList(Seq()),
          Option.empty,
          Option.empty,
          Option.empty,
          external = false),
        Map.empty,
        ignoreIfExists = true,
        isAnalyzed = false)
    // Compare the two plans
    assert(compareByString(logPlan) == expectedPlan.toString)
  }

  test(
    "test project a simple search with only one table using parquet and Options with multiple partitioned fields ") {
    // if successful build ppl logical plan and translate to catalyst logical plan
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(
      plan(
        pplParser,
        "project if not exists simpleView using parquet OPTIONS('parquet.bloom.filter.enabled'='true', 'parquet.bloom.filter.enabled#age'='false') " +
          " partitioned by (age, country) | source = table | where state != 'California' "),
      context)

    // Define the expected logical plan
    val relation = UnresolvedRelation(Seq("table"))
    val filter =
      Filter(Not(EqualTo(UnresolvedAttribute("state"), Literal("California"))), relation)
    val expectedPlan: LogicalPlan =
      CreateTableAsSelect(
        UnresolvedIdentifier(Seq("simpleView")),
        Seq(),
//        Seq(IdentityTransform.apply(FieldReference.apply("age")), IdentityTransform.apply(FieldReference.apply("country"))),
        Project(Seq(UnresolvedStar(None)), filter),
        UnresolvedTableSpec(
          Map.empty,
          Option("PARQUET"),
          OptionList(
            Seq(
              ("parquet.bloom.filter.enabled", Literal("true")),
              ("parquet.bloom.filter.enabled#age", Literal("false")))),
          Option.empty,
          Option.empty,
          Option.empty,
          external = false),
        Map.empty,
        ignoreIfExists = true,
        isAnalyzed = false)
    // Compare the two plans
    assert(compareByString(logPlan) == expectedPlan.toString)
  }

  test(
    "test project a simple search with only one table using parquet with location and Options with multiple partitioned fields ") {
    val viewLocation = viewFolderLocation.toAbsolutePath.toString
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(
      plan(
        pplParser,
        s"""
         | project if not exists simpleView using parquet OPTIONS('parquet.bloom.filter.enabled'='true', 'parquet.bloom.filter.enabled#age'='false')
         | partitioned by (age, country) location '$viewLocation' | source = table | where state != 'California'
        """.stripMargin),
      context)

    // Define the expected logical plan
    val relation = UnresolvedRelation(Seq("table"))
    val filter =
      Filter(Not(EqualTo(UnresolvedAttribute("state"), Literal("California"))), relation)
    val expectedPlan: LogicalPlan =
      CreateTableAsSelect(
        UnresolvedIdentifier(Seq("simpleView")),
        Seq(),
//        Seq(IdentityTransform.apply(FieldReference.apply("age")), IdentityTransform.apply(FieldReference.apply("country"))),
        Project(Seq(UnresolvedStar(None)), filter),
        UnresolvedTableSpec(
          Map.empty,
          Option("PARQUET"),
          OptionList(
            Seq(
              ("parquet.bloom.filter.enabled", Literal("true")),
              ("parquet.bloom.filter.enabled#age", Literal("false")))),
          Option(viewLocation),
          Option.empty,
          Option.empty,
          external = false),
        Map.empty,
        ignoreIfExists = true,
        isAnalyzed = false)
    // Compare the two plans
    assert(compareByString(logPlan) == expectedPlan.toString)
  }

  test(
    "test project with inner join: join condition with table names and predicates using parquet with location and Options with single partitioned fields") {
    val viewLocation = viewFolderLocation.toAbsolutePath.toString
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(
      plan(
        pplParser,
        s"""
         | project if not exists simpleView using parquet OPTIONS('parquet.bloom.filter.enabled'='true')
         | partitioned by (name) location '$viewLocation' |
         | source = $testTable1| INNER JOIN left = l right = r ON $testTable1.id = $testTable2.id AND $testTable1.count > 10 AND lower($testTable2.name) = 'hello' $testTable2
         | """.stripMargin),
      context)
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
    val expectedPlan: LogicalPlan =
      CreateTableAsSelect(
        UnresolvedIdentifier(Seq("simpleView")),
        Seq(),
        //        Seq(IdentityTransform.apply(FieldReference.apply("age")), IdentityTransform.apply(FieldReference.apply("country"))),
        Project(Seq(UnresolvedStar(None)), joinPlan),
        UnresolvedTableSpec(
          Map.empty,
          Option("PARQUET"),
          OptionList(Seq(("parquet.bloom.filter.enabled", Literal("true")))),
          Option(viewLocation),
          Option.empty,
          Option.empty,
          external = false),
        Map.empty,
        ignoreIfExists = true,
        isAnalyzed = false)

    // Compare the two plans
    comparePlans(
      logPlan.asInstanceOf[CreateTableAsSelect].query,
      expectedPlan.asInstanceOf[CreateTableAsSelect].query,
      checkAnalysis = false)
    comparePlans(
      logPlan.asInstanceOf[CreateTableAsSelect].name,
      expectedPlan.asInstanceOf[CreateTableAsSelect].name,
      checkAnalysis = false)
    assert(
      logPlan.asInstanceOf[CreateTableAsSelect].tableSpec.toString == expectedPlan
        .asInstanceOf[CreateTableAsSelect]
        .tableSpec
        .toString)
  }
}
