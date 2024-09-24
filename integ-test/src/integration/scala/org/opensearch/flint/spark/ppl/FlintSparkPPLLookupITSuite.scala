/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import org.opensearch.sql.ppl.utils.DataTypeTransformer.seq

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Coalesce, EqualTo}
import org.apache.spark.sql.catalyst.plans.LeftOuter
import org.apache.spark.sql.catalyst.plans.logical.{DataFrameDropColumns, Join, JoinHint, LogicalPlan, Project, SubqueryAlias}
import org.apache.spark.sql.streaming.StreamTest

class FlintSparkPPLLookupITSuite
    extends QueryTest
    with LogicalPlanTestUtils
    with FlintPPLSuite
    with StreamTest {

  /** Test table and index name */
  private val sourceTable = "spark_catalog.default.flint_ppl_test1"
  private val lookupTable = "spark_catalog.default.flint_ppl_test2"

  override def beforeAll(): Unit = {
    super.beforeAll()
    createPeopleTable(sourceTable)
    createWorkInformationTable(lookupTable)
  }

  protected override def afterEach(): Unit = {
    super.afterEach()
    // Stop all streaming jobs if any
    spark.streams.active.foreach { job =>
      job.stop()
      job.awaitTermination()
    }
  }

  private def sourceAlias: LogicalPlan = {
    val tbl = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test1"))
    SubqueryAlias("__auto_generated_subquery_name_s", tbl)
  }

  private def lookupAlias: LogicalPlan = {
    val tbl = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test2"))
    SubqueryAlias("__auto_generated_subquery_name_l", tbl)
  }

  test("test LOOKUP lookupTable uid AS id REPLACE department") {
    val frame = sql(s"source = $sourceTable| LOOKUP $lookupTable uid AS id REPLACE department")
    // frame.show()
    // frame.explain(true)
    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] = Array(
      Row(1000, "Jake", "Engineer", "England", 100000, "IT"),
      Row(1001, "Hello", "Artist", "USA", 70000, null),
      Row(1002, "John", "Doctor", "Canada", 120000, "DATA"),
      Row(1003, "David", "Doctor", null, 120000, "HR"),
      Row(1004, "David", null, "Canada", 0, null),
      Row(1005, "Jane", "Scientist", "Canada", 90000, "DATA"))

    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Integer](_.getAs[Integer](0))
    assert(results.sorted.sameElements(expectedResults.sorted))
    val lookupProject =
      Project(Seq(UnresolvedAttribute("department"), UnresolvedAttribute("uid")), lookupAlias)
    val joinCondition = EqualTo(UnresolvedAttribute("uid"), UnresolvedAttribute("id"))
    val joinPlan = Join(sourceAlias, lookupProject, LeftOuter, Some(joinCondition), JoinHint.NONE)
    val coalesceForSafeExpr =
      Coalesce(Seq(UnresolvedAttribute("department"), UnresolvedAttribute("department")))
    val projectAfterJoin = Project(
      Seq(
        UnresolvedStar(Some(Seq("__auto_generated_subquery_name_s"))),
        Alias(coalesceForSafeExpr, "department")()),
      joinPlan)
    val dropColumns = DataFrameDropColumns(
      Seq(
        UnresolvedAttribute("uid"),
        UnresolvedAttribute("__auto_generated_subquery_name_s.department")),
      projectAfterJoin)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), dropColumns)
    val logicalPlan: LogicalPlan = frame.queryExecution.logical

    comparePlans(expectedPlan, logicalPlan, checkAnalysis = false)
  }

  test("test LOOKUP lookupTable uid AS id APPEND department") {
    val frame = sql(s"source = $sourceTable| LOOKUP $lookupTable uid AS id APPEND department")
    // frame.show()
    // frame.explain(true)
    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] = Array(
      Row(1000, "Jake", "Engineer", "England", 100000, "IT"),
      Row(1001, "Hello", "Artist", "USA", 70000, null),
      Row(1002, "John", "Doctor", "Canada", 120000, "DATA"),
      Row(1003, "David", "Doctor", null, 120000, "HR"),
      Row(1004, "David", null, "Canada", 0, null),
      Row(1005, "Jane", "Scientist", "Canada", 90000, "DATA"))

    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Integer](_.getAs[Integer](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    val lookupProject =
      Project(Seq(UnresolvedAttribute("department"), UnresolvedAttribute("uid")), lookupAlias)
    val joinCondition = EqualTo(UnresolvedAttribute("uid"), UnresolvedAttribute("id"))
    val joinPlan = Join(sourceAlias, lookupProject, LeftOuter, Some(joinCondition), JoinHint.NONE)
    val coalesceExpr =
      Coalesce(Seq(UnresolvedAttribute("department"), UnresolvedAttribute("department")))
    val coalesceForSafeExpr = Coalesce(Seq(coalesceExpr, UnresolvedAttribute("department")))
    val projectAfterJoin = Project(
      Seq(
        UnresolvedStar(Some(Seq("__auto_generated_subquery_name_s"))),
        Alias(coalesceForSafeExpr, "department")()),
      joinPlan)
    val dropColumns = DataFrameDropColumns(
      Seq(
        UnresolvedAttribute("uid"),
        UnresolvedAttribute("__auto_generated_subquery_name_s.department")),
      projectAfterJoin)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), dropColumns)
    val logicalPlan: LogicalPlan = frame.queryExecution.logical

    comparePlans(expectedPlan, logicalPlan, checkAnalysis = false)
  }

  test("test LOOKUP lookupTable uid AS id REPLACE department AS country") {
    val frame =
      sql(s"source = $sourceTable| LOOKUP $lookupTable uid AS id REPLACE department AS country")
    // frame.show()
    // frame.explain(true)
    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] = Array(
      Row(1000, "Jake", "Engineer", 100000, "IT"),
      Row(1001, "Hello", "Artist", 70000, "USA"),
      Row(1002, "John", "Doctor", 120000, "DATA"),
      Row(1003, "David", "Doctor", 120000, "HR"),
      Row(1004, "David", null, 0, "Canada"),
      Row(1005, "Jane", "Scientist", 90000, "DATA"))

    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Integer](_.getAs[Integer](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    val lookupProject =
      Project(Seq(UnresolvedAttribute("department"), UnresolvedAttribute("uid")), lookupAlias)
    val joinCondition = EqualTo(UnresolvedAttribute("uid"), UnresolvedAttribute("id"))
    val joinPlan = Join(sourceAlias, lookupProject, LeftOuter, Some(joinCondition), JoinHint.NONE)
    val coalesceForSafeExpr =
      Coalesce(Seq(UnresolvedAttribute("department"), UnresolvedAttribute("country")))
    val projectAfterJoin = Project(
      Seq(
        UnresolvedStar(Some(Seq("__auto_generated_subquery_name_s"))),
        Alias(coalesceForSafeExpr, "country")()),
      joinPlan)
    val dropColumns = DataFrameDropColumns(
      Seq(
        UnresolvedAttribute("uid"),
        UnresolvedAttribute("__auto_generated_subquery_name_s.country")),
      projectAfterJoin)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), dropColumns)
    val logicalPlan: LogicalPlan = frame.queryExecution.logical

    comparePlans(expectedPlan, logicalPlan, checkAnalysis = false)
  }

  test("test LOOKUP lookupTable uid AS id APPEND department AS country") {
    val frame =
      sql(s"source = $sourceTable| LOOKUP $lookupTable uid AS id APPEND department AS country")
    // frame.show()
    // frame.explain(true)
    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] = Array(
      Row(1000, "Jake", "Engineer", 100000, "England"),
      Row(1001, "Hello", "Artist", 70000, "USA"),
      Row(1002, "John", "Doctor", 120000, "Canada"),
      Row(1003, "David", "Doctor", 120000, "HR"),
      Row(1004, "David", null, 0, "Canada"),
      Row(1005, "Jane", "Scientist", 90000, "Canada"))

    val lookupProject =
      Project(Seq(UnresolvedAttribute("department"), UnresolvedAttribute("uid")), lookupAlias)
    val joinCondition = EqualTo(UnresolvedAttribute("uid"), UnresolvedAttribute("id"))
    val joinPlan = Join(sourceAlias, lookupProject, LeftOuter, Some(joinCondition), JoinHint.NONE)
    val coalesceExpr =
      Coalesce(Seq(UnresolvedAttribute("country"), UnresolvedAttribute("department")))
    val coalesceForSafeExpr = Coalesce(Seq(coalesceExpr, UnresolvedAttribute("country")))
    val projectAfterJoin = Project(
      Seq(
        UnresolvedStar(Some(Seq("__auto_generated_subquery_name_s"))),
        Alias(coalesceForSafeExpr, "country")()),
      joinPlan)
    val dropColumns = DataFrameDropColumns(
      Seq(
        UnresolvedAttribute("uid"),
        UnresolvedAttribute("__auto_generated_subquery_name_s.country")),
      projectAfterJoin)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), dropColumns)
    val logicalPlan: LogicalPlan = frame.queryExecution.logical

    comparePlans(expectedPlan, logicalPlan, checkAnalysis = false)
  }

  test("test LOOKUP lookupTable uid AS id, name REPLACE department") {
    val frame =
      sql(s"source = $sourceTable| LOOKUP $lookupTable uID AS id, name REPLACE department")
    // frame.show()
    // frame.explain(true)
    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] = Array(
      Row(1000, "Jake", "Engineer", "England", 100000, "IT"),
      Row(1001, "Hello", "Artist", "USA", 70000, null),
      Row(1002, "John", "Doctor", "Canada", 120000, "DATA"),
      Row(1003, "David", "Doctor", null, 120000, "HR"),
      Row(1004, "David", null, "Canada", 0, null),
      Row(1005, "Jane", "Scientist", "Canada", 90000, "DATA"))

    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Integer](_.getAs[Integer](0))
    assert(results.sorted.sameElements(expectedResults.sorted))
    val lookupProject =
      Project(
        Seq(
          UnresolvedAttribute("department"),
          UnresolvedAttribute("uID"),
          UnresolvedAttribute("name")),
        lookupAlias)
    val joinCondition =
      And(
        EqualTo(UnresolvedAttribute("uID"), UnresolvedAttribute("id")),
        EqualTo(
          UnresolvedAttribute("__auto_generated_subquery_name_l.name"),
          UnresolvedAttribute("__auto_generated_subquery_name_s.name")))
    val joinPlan = Join(sourceAlias, lookupProject, LeftOuter, Some(joinCondition), JoinHint.NONE)
    val coalesceForSafeExpr =
      Coalesce(Seq(UnresolvedAttribute("department"), UnresolvedAttribute("department")))
    val projectAfterJoin = Project(
      Seq(
        UnresolvedStar(Some(Seq("__auto_generated_subquery_name_s"))),
        Alias(coalesceForSafeExpr, "department")()),
      joinPlan)
    val dropColumns = DataFrameDropColumns(
      Seq(
        UnresolvedAttribute("uID"),
        UnresolvedAttribute("__auto_generated_subquery_name_l.name"),
        UnresolvedAttribute("__auto_generated_subquery_name_s.department")),
      projectAfterJoin)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), dropColumns)
    val logicalPlan: LogicalPlan = frame.queryExecution.logical

    comparePlans(expectedPlan, logicalPlan, checkAnalysis = false)
  }

  test("test LOOKUP lookupTable uid AS id, name APPEND department") {
    val frame =
      sql(s"source = $sourceTable| LOOKUP $lookupTable uid AS ID, name APPEND department")
    // frame.show()
    // frame.explain(true)
    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] = Array(
      Row(1000, "Jake", "Engineer", "England", 100000, "IT"),
      Row(1001, "Hello", "Artist", "USA", 70000, null),
      Row(1002, "John", "Doctor", "Canada", 120000, "DATA"),
      Row(1003, "David", "Doctor", null, 120000, "HR"),
      Row(1004, "David", null, "Canada", 0, null),
      Row(1005, "Jane", "Scientist", "Canada", 90000, "DATA"))

    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Integer](_.getAs[Integer](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    val lookupProject =
      Project(
        Seq(
          UnresolvedAttribute("department"),
          UnresolvedAttribute("uid"),
          UnresolvedAttribute("name")),
        lookupAlias)
    val joinCondition =
      And(
        EqualTo(UnresolvedAttribute("uid"), UnresolvedAttribute("ID")),
        EqualTo(
          UnresolvedAttribute("__auto_generated_subquery_name_l.name"),
          UnresolvedAttribute("__auto_generated_subquery_name_s.name")))
    val joinPlan = Join(sourceAlias, lookupProject, LeftOuter, Some(joinCondition), JoinHint.NONE)
    val coalesceExpr =
      Coalesce(Seq(UnresolvedAttribute("department"), UnresolvedAttribute("department")))
    val coalesceForSafeExpr = Coalesce(Seq(coalesceExpr, UnresolvedAttribute("department")))
    val projectAfterJoin = Project(
      Seq(
        UnresolvedStar(Some(Seq("__auto_generated_subquery_name_s"))),
        Alias(coalesceForSafeExpr, "department")()),
      joinPlan)
    val dropColumns = DataFrameDropColumns(
      Seq(
        UnresolvedAttribute("uid"),
        UnresolvedAttribute("__auto_generated_subquery_name_l.name"),
        UnresolvedAttribute("__auto_generated_subquery_name_s.department")),
      projectAfterJoin)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), dropColumns)
    val logicalPlan: LogicalPlan = frame.queryExecution.logical

    comparePlans(expectedPlan, logicalPlan, checkAnalysis = false)
  }

  test("test LOOKUP lookupTable uid AS id, name") {
    val frame = sql(s"source = $sourceTable| LOOKUP $lookupTable uID AS id, name")
    // frame.show()
    // frame.explain(true)
    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] = Array(
      Row(1000, "Jake", "Engineer", "England", 100000, 1000, "Jake", "IT", "Engineer"),
      Row(1001, "Hello", "Artist", "USA", 70000, null, null, null, null),
      Row(1002, "John", "Doctor", "Canada", 120000, 1002, "John", "DATA", "Scientist"),
      Row(1003, "David", "Doctor", null, 120000, 1003, "David", "HR", "Doctor"),
      Row(1004, "David", null, "Canada", 0, null, null, null, null),
      Row(1005, "Jane", "Scientist", "Canada", 90000, 1005, "Jane", "DATA", "Engineer"))

    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Integer](_.getAs[Integer](0))
    assert(results.sorted.sameElements(expectedResults.sorted))
  }

  test("test LOOKUP lookupTable name REPLACE occupation") {
    val frame =
      sql(
        s"source = $sourceTable | eval major = occupation | fields id, name, major, country, salary | LOOKUP $lookupTable name REPLACE occupation AS major")
    // frame.show()
    // frame.explain(true)
    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] = Array(
      Row(1000, "Jake", "England", 100000, "Engineer"),
      Row(1001, "Hello", "USA", 70000, "Artist"),
      Row(1002, "John", "Canada", 120000, "Scientist"),
      Row(1003, "David", null, 120000, "Doctor"),
      Row(1004, "David", "Canada", 0, "Doctor"),
      Row(1005, "Jane", "Canada", 90000, "Engineer"))

    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Integer](_.getAs[Integer](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    val sourceTbl = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test1"))
    val eval = Project(
      seq(UnresolvedStar(None), Alias(UnresolvedAttribute("occupation"), "major")()),
      sourceTbl)
    val fields = Project(
      seq(
        UnresolvedAttribute("id"),
        UnresolvedAttribute("name"),
        UnresolvedAttribute("major"),
        UnresolvedAttribute("country"),
        UnresolvedAttribute("salary")),
      eval)
    val sourceAlias = SubqueryAlias("__auto_generated_subquery_name_s", fields)
    val lookupProject =
      Project(Seq(UnresolvedAttribute("occupation"), UnresolvedAttribute("name")), lookupAlias)
    val joinCondition = EqualTo(
      UnresolvedAttribute("__auto_generated_subquery_name_s.name"),
      UnresolvedAttribute("__auto_generated_subquery_name_l.name"))
    val joinPlan = Join(sourceAlias, lookupProject, LeftOuter, Some(joinCondition), JoinHint.NONE)
    val coalesceForSafeExpr =
      Coalesce(Seq(UnresolvedAttribute("occupation"), UnresolvedAttribute("major")))
    val projectAfterJoin = Project(
      Seq(
        UnresolvedStar(Some(Seq("__auto_generated_subquery_name_s"))),
        Alias(coalesceForSafeExpr, "major")()),
      joinPlan)
    val dropColumns = DataFrameDropColumns(
      Seq(
        UnresolvedAttribute("__auto_generated_subquery_name_l.name"),
        UnresolvedAttribute("__auto_generated_subquery_name_s.major")),
      projectAfterJoin)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), dropColumns)
    val logicalPlan: LogicalPlan = frame.queryExecution.logical

    comparePlans(expectedPlan, logicalPlan, checkAnalysis = false)
  }

  test("test LOOKUP lookupTable name APPEND occupation") {
    val frame =
      sql(
        s"source = $sourceTable | eval major = occupation | fields id, name, major, country, salary | LOOKUP $lookupTable name APPEND occupation AS major")
    // frame.show()
    // frame.explain(true)
    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] = Array(
      Row(1000, "Jake", "England", 100000, "Engineer"),
      Row(1001, "Hello", "USA", 70000, "Artist"),
      Row(1002, "John", "Canada", 120000, "Doctor"),
      Row(1003, "David", null, 120000, "Doctor"),
      Row(1004, "David", "Canada", 0, "Doctor"),
      Row(1005, "Jane", "Canada", 90000, "Scientist"))

    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Integer](_.getAs[Integer](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    val sourceTbl = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test1"))
    val eval = Project(
      seq(UnresolvedStar(None), Alias(UnresolvedAttribute("occupation"), "major")()),
      sourceTbl)
    val fields = Project(
      seq(
        UnresolvedAttribute("id"),
        UnresolvedAttribute("name"),
        UnresolvedAttribute("major"),
        UnresolvedAttribute("country"),
        UnresolvedAttribute("salary")),
      eval)
    val sourceAlias = SubqueryAlias("__auto_generated_subquery_name_s", fields)
    val lookupProject =
      Project(Seq(UnresolvedAttribute("occupation"), UnresolvedAttribute("name")), lookupAlias)
    val joinCondition = EqualTo(
      UnresolvedAttribute("__auto_generated_subquery_name_s.name"),
      UnresolvedAttribute("__auto_generated_subquery_name_l.name"))
    val joinPlan = Join(sourceAlias, lookupProject, LeftOuter, Some(joinCondition), JoinHint.NONE)
    val coalesceExpr =
      Coalesce(Seq(UnresolvedAttribute("major"), UnresolvedAttribute("occupation")))
    val coalesceForSafeExpr =
      Coalesce(Seq(coalesceExpr, UnresolvedAttribute("major")))
    val projectAfterJoin = Project(
      Seq(
        UnresolvedStar(Some(Seq("__auto_generated_subquery_name_s"))),
        Alias(coalesceForSafeExpr, "major")()),
      joinPlan)
    val dropColumns = DataFrameDropColumns(
      Seq(
        UnresolvedAttribute("__auto_generated_subquery_name_l.name"),
        UnresolvedAttribute("__auto_generated_subquery_name_s.major")),
      projectAfterJoin)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), dropColumns)
    val logicalPlan: LogicalPlan = frame.queryExecution.logical

    comparePlans(expectedPlan, logicalPlan, checkAnalysis = false)
  }
}
