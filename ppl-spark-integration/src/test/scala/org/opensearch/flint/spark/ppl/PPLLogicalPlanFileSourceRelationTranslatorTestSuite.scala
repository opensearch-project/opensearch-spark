/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import org.opensearch.flint.spark.ppl.PlaneUtils.plan
import org.opensearch.sql.ppl.{CatalystPlanContext, CatalystQueryPlanVisitor}
import org.scalatest.matchers.should.Matchers

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.UnresolvedStar
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.execution.datasources.{DataSource, LogicalRelation}
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructType}

class PPLLogicalPlanFileSourceRelationTranslatorTestSuite
    extends SparkFunSuite
    with PlanTest
    with LogicalPlanTestUtils
    with Matchers {

  private val spark = SparkSession.builder().master("local").getOrCreate()
  private val planTransformer = new CatalystQueryPlanVisitor(spark)
  private val pplParser = new PPLSyntaxParser()

  test("test csv file source relation") {
    val context = new CatalystPlanContext
    val scanPlan = planTransformer.visit(
      plan(pplParser, "file=test1(ppl-spark-integration/src/test/resources/people.csv)", false),
      context)

    val projectList: Seq[NamedExpression] = Seq(UnresolvedStar(None))
    val userSpecifiedSchema = new StructType()
      .add("name", StringType)
      .add("age", IntegerType)
      .add("job", StringType)
    val dataSource =
      DataSource(
        spark,
        userSpecifiedSchema = Some(userSpecifiedSchema),
        className = "csv",
        options = Map.empty)
    val relation = LogicalRelation(dataSource.resolveRelation(true), isStreaming = false)
    val expectedPlan = Project(projectList, relation)
    assert(compareByString(expectedPlan) === compareByString(scanPlan))
  }

  test("test gz compressed csv file source relation") {
    val context = new CatalystPlanContext
    val scanPlan = planTransformer.visit(
      plan(
        pplParser,
        "file=test1(ppl-spark-integration/src/test/resources/people.csv.gz)",
        false),
      context)

    val projectList: Seq[NamedExpression] = Seq(UnresolvedStar(None))
    val userSpecifiedSchema = new StructType()
      .add("name", StringType)
      .add("age", IntegerType)
      .add("job", StringType)
    val dataSource =
      DataSource(
        spark,
        userSpecifiedSchema = Some(userSpecifiedSchema),
        className = "csv",
        options = Map.empty)
    val relation = LogicalRelation(dataSource.resolveRelation(true), isStreaming = false)
    val expectedPlan = Project(projectList, relation)
    assert(compareByString(expectedPlan) === compareByString(scanPlan))
  }

  test("test unsupported compression") {
    val context = new CatalystPlanContext
    val thrown = intercept[UnsupportedOperationException] {
      planTransformer.visit(
        plan(
          pplParser,
          "file=test1(ppl-spark-integration/src/test/resources/head.csv.brotli)",
          false),
        context)
    }

    assert(thrown.getMessage.startsWith("Unsupported file suffix: brotli"))
  }

  test("test unsupported file format") {
    val context = new CatalystPlanContext
    val thrown = intercept[UnsupportedOperationException] {
      planTransformer.visit(
        plan(pplParser, "file=test1(ppl-spark-integration/src/test/resources/head.txt)", false),
        context)
    }

    assert(thrown.getMessage.startsWith("Unsupported file suffix: txt"))
  }
}
