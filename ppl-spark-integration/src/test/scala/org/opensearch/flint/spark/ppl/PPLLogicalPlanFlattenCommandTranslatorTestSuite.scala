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
import org.apache.spark.sql.catalyst.expressions.{Alias, GeneratorOuter, Literal, NamedExpression}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{DataFrameDropColumns, Generate, Project}

class PPLLogicalPlanFlattenCommandTranslatorTestSuite
    extends SparkFunSuite
    with PlanTest
    with LogicalPlanTestUtils
    with Matchers {

  private val planTransformer = new CatalystQueryPlanVisitor()
  private val pplParser = new PPLSyntaxParser()

  test("test fillnull only field") {
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

}
