/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import org.junit.Assert.assertEquals
import org.opensearch.flint.spark.ppl.PlaneUtils.plan
import org.opensearch.sql.ppl.{CatalystPlanContext, CatalystQueryPlanVisitor}
import org.scalatest.matchers.should.Matchers

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{Ascending, Descending, Literal, NamedExpression, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical._

class PPLLogicalPlanCorrelationQueriesTranslatorTestSuite
    extends SparkFunSuite
    with LogicalPlanTestUtils
    with Matchers {

  private val planTrnasformer = new CatalystQueryPlanVisitor()
  private val pplParser = new PPLSyntaxParser()

  ignore("Search multiple tables with correlation - translated into join call with fields") {
    val context = new CatalystPlanContext
    val query = "source = table1, table2 | correlate exact fields(ip, port) scope(@timestamp, 1d)"
    val logPlan = planTrnasformer.visit(plan(pplParser, query, false), context)

    val table1 = UnresolvedRelation(Seq("table1"))
    val table2 = UnresolvedRelation(Seq("table2"))

    val allFields1 = UnresolvedStar(None)
    val allFields2 = UnresolvedStar(None)

    val projectedTable1 = Project(Seq(allFields1), table1)
    val projectedTable2 = Project(Seq(allFields2), table2)

    val expectedPlan =
      Union(Seq(projectedTable1, projectedTable2), byName = true, allowMissingCol = true)

    assertEquals(expectedPlan, logPlan)
  }
  ignore(
    "Search multiple tables with correlation with filters - translated into join call with fields") {
    val context = new CatalystPlanContext
    val query =
      "source = table1, table2 | where @timestamp=`2018-07-02T22:23:00` AND ip=`10.0.0.1` AND cloud.provider=`aws` | correlate exact fields(ip, port) scope(@timestamp, 1d)"
    val logPlan = planTrnasformer.visit(plan(pplParser, query, false), context)

    val table1 = UnresolvedRelation(Seq("table1"))
    val table2 = UnresolvedRelation(Seq("table2"))

    val allFields1 = UnresolvedStar(None)
    val allFields2 = UnresolvedStar(None)

    val projectedTable1 = Project(Seq(allFields1), table1)
    val projectedTable2 = Project(Seq(allFields2), table2)

    val expectedPlan =
      Union(Seq(projectedTable1, projectedTable2), byName = true, allowMissingCol = true)

    assertEquals(expectedPlan, logPlan)
  }
  ignore(
    "Search multiple tables with correlation - translated into join call with different fields mapping ") {
    val context = new CatalystPlanContext
    val query =
      "source = table1, table2 | where @timestamp=`2018-07-02T22:23:00` AND ip=`10.0.0.1` AND cloud.provider=`aws` | correlate exact fields(ip, port) scope(@timestamp, 1d)" +
        " mapping( alb_logs.ip = traces.source_ip, alb_logs.port = metrics.target_port )"
    val logPlan = planTrnasformer.visit(plan(pplParser, query, false), context)

    val table1 = UnresolvedRelation(Seq("table1"))
    val table2 = UnresolvedRelation(Seq("table2"))

    val allFields1 = UnresolvedStar(None)
    val allFields2 = UnresolvedStar(None)

    val projectedTable1 = Project(Seq(allFields1), table1)
    val projectedTable2 = Project(Seq(allFields2), table2)

    val expectedPlan =
      Union(Seq(projectedTable1, projectedTable2), byName = true, allowMissingCol = true)

    assertEquals(expectedPlan, logPlan)
  }
}
