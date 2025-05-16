/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import org.opensearch.sql.ppl.utils.DataTypeTransformer.seq

import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
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
  private val sourceTable2 = "spark_catalog.default.flint_ppl_test3"

  override def beforeAll(): Unit = {
    super.beforeAll()
    createPeopleTable(sourceTable)
    createWorkInformationTable(lookupTable)
    createPeopleTable(sourceTable2)

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
    val expectedResults: Array[Row] = Array(
      Row(1000, "Jake", "Engineer", "England", 100000, "IT"),
      Row(1001, "Hello", "Artist", "USA", 70000, null),
      Row(1002, "John", "Doctor", "Canada", 120000, "DATA"),
      Row(1003, "David", "Doctor", null, 120000, "HR"),
      Row(1004, "David", null, "Canada", 0, null),
      Row(1005, "Jane", "Scientist", "Canada", 90000, "DATA"))
    assertSameRows(expectedResults, frame)

    val lookupProject =
      Project(Seq(UnresolvedAttribute("department"), UnresolvedAttribute("uid")), lookupAlias)
    val joinCondition = EqualTo(
      UnresolvedAttribute("__auto_generated_subquery_name_l.uid"),
      UnresolvedAttribute("__auto_generated_subquery_name_s.id"))
    val joinPlan = Join(sourceAlias, lookupProject, LeftOuter, Some(joinCondition), JoinHint.NONE)
    val projectAfterJoin = Project(
      Seq(
        UnresolvedStar(Some(Seq("__auto_generated_subquery_name_s"))),
        Alias(
          UnresolvedAttribute("__auto_generated_subquery_name_l.department"),
          "department")()),
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
    val expectedResults: Array[Row] = Array(
      Row(1000, "Jake", "Engineer", "England", 100000, "IT"),
      Row(1001, "Hello", "Artist", "USA", 70000, null),
      Row(1002, "John", "Doctor", "Canada", 120000, "DATA"),
      Row(1003, "David", "Doctor", null, 120000, "HR"),
      Row(1004, "David", null, "Canada", 0, null),
      Row(1005, "Jane", "Scientist", "Canada", 90000, "DATA"))
    assertSameRows(expectedResults, frame)

    val lookupProject =
      Project(Seq(UnresolvedAttribute("department"), UnresolvedAttribute("uid")), lookupAlias)
    val joinCondition = EqualTo(
      UnresolvedAttribute("__auto_generated_subquery_name_l.uid"),
      UnresolvedAttribute("__auto_generated_subquery_name_s.id"))
    val joinPlan = Join(sourceAlias, lookupProject, LeftOuter, Some(joinCondition), JoinHint.NONE)
    val coalesceExpr =
      Coalesce(
        Seq(
          UnresolvedAttribute("department"),
          UnresolvedAttribute("__auto_generated_subquery_name_l.department")))
    val projectAfterJoin = Project(
      Seq(
        UnresolvedStar(Some(Seq("__auto_generated_subquery_name_s"))),
        Alias(coalesceExpr, "department")()),
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
    val expectedResults: Array[Row] = Array(
      Row(1000, "Jake", "Engineer", 100000, "IT"),
      Row(1001, "Hello", "Artist", 70000, null),
      Row(1002, "John", "Doctor", 120000, "DATA"),
      Row(1003, "David", "Doctor", 120000, "HR"),
      Row(1004, "David", null, 0, null),
      Row(1005, "Jane", "Scientist", 90000, "DATA"))
    assertSameRows(expectedResults, frame)

    val lookupProject =
      Project(Seq(UnresolvedAttribute("department"), UnresolvedAttribute("uid")), lookupAlias)
    val joinCondition = EqualTo(
      UnresolvedAttribute("__auto_generated_subquery_name_l.uid"),
      UnresolvedAttribute("__auto_generated_subquery_name_s.id"))
    val joinPlan = Join(sourceAlias, lookupProject, LeftOuter, Some(joinCondition), JoinHint.NONE)
    val projectAfterJoin = Project(
      Seq(
        UnresolvedStar(Some(Seq("__auto_generated_subquery_name_s"))),
        Alias(UnresolvedAttribute("__auto_generated_subquery_name_l.department"), "country")()),
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
    val expectedResults: Array[Row] = Array(
      Row(1000, "Jake", "Engineer", 100000, "England"),
      Row(1001, "Hello", "Artist", 70000, "USA"),
      Row(1002, "John", "Doctor", 120000, "Canada"),
      Row(1003, "David", "Doctor", 120000, "HR"),
      Row(1004, "David", null, 0, "Canada"),
      Row(1005, "Jane", "Scientist", 90000, "Canada"))
    assertSameRows(expectedResults, frame)

    val lookupProject =
      Project(Seq(UnresolvedAttribute("department"), UnresolvedAttribute("uid")), lookupAlias)
    val joinCondition = EqualTo(
      UnresolvedAttribute("__auto_generated_subquery_name_l.uid"),
      UnresolvedAttribute("__auto_generated_subquery_name_s.id"))
    val joinPlan = Join(sourceAlias, lookupProject, LeftOuter, Some(joinCondition), JoinHint.NONE)
    val coalesceExpr =
      Coalesce(
        Seq(
          UnresolvedAttribute("__auto_generated_subquery_name_s.country"),
          UnresolvedAttribute("__auto_generated_subquery_name_l.department")))
    val projectAfterJoin = Project(
      Seq(
        UnresolvedStar(Some(Seq("__auto_generated_subquery_name_s"))),
        Alias(coalesceExpr, "country")()),
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
      sql(s"source = $sourceTable| LOOKUP $lookupTable uid AS id, name REPLACE department")
    val expectedResults: Array[Row] = Array(
      Row(1000, "Jake", "Engineer", "England", 100000, "IT"),
      Row(1001, "Hello", "Artist", "USA", 70000, null),
      Row(1002, "John", "Doctor", "Canada", 120000, "DATA"),
      Row(1003, "David", "Doctor", null, 120000, "HR"),
      Row(1004, "David", null, "Canada", 0, null),
      Row(1005, "Jane", "Scientist", "Canada", 90000, "DATA"))
    assertSameRows(expectedResults, frame)

    val lookupProject =
      Project(
        Seq(
          UnresolvedAttribute("department"),
          UnresolvedAttribute("uid"),
          UnresolvedAttribute("name")),
        lookupAlias)
    val joinCondition =
      And(
        EqualTo(
          UnresolvedAttribute("__auto_generated_subquery_name_l.uid"),
          UnresolvedAttribute("__auto_generated_subquery_name_s.id")),
        EqualTo(
          UnresolvedAttribute("__auto_generated_subquery_name_l.name"),
          UnresolvedAttribute("__auto_generated_subquery_name_s.name")))
    val joinPlan = Join(sourceAlias, lookupProject, LeftOuter, Some(joinCondition), JoinHint.NONE)
    val projectAfterJoin = Project(
      Seq(
        UnresolvedStar(Some(Seq("__auto_generated_subquery_name_s"))),
        Alias(
          UnresolvedAttribute("__auto_generated_subquery_name_l.department"),
          "department")()),
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

  test("test LOOKUP lookupTable uid AS id, name APPEND department") {
    val frame =
      sql(s"source = $sourceTable| LOOKUP $lookupTable uid AS id, name APPEND department")
    val expectedResults: Array[Row] = Array(
      Row(1000, "Jake", "Engineer", "England", 100000, "IT"),
      Row(1001, "Hello", "Artist", "USA", 70000, null),
      Row(1002, "John", "Doctor", "Canada", 120000, "DATA"),
      Row(1003, "David", "Doctor", null, 120000, "HR"),
      Row(1004, "David", null, "Canada", 0, null),
      Row(1005, "Jane", "Scientist", "Canada", 90000, "DATA"))
    assertSameRows(expectedResults, frame)

    val lookupProject =
      Project(
        Seq(
          UnresolvedAttribute("department"),
          UnresolvedAttribute("uid"),
          UnresolvedAttribute("name")),
        lookupAlias)
    val joinCondition =
      And(
        EqualTo(
          UnresolvedAttribute("__auto_generated_subquery_name_l.uid"),
          UnresolvedAttribute("__auto_generated_subquery_name_s.id")),
        EqualTo(
          UnresolvedAttribute("__auto_generated_subquery_name_l.name"),
          UnresolvedAttribute("__auto_generated_subquery_name_s.name")))
    val joinPlan = Join(sourceAlias, lookupProject, LeftOuter, Some(joinCondition), JoinHint.NONE)
    val coalesceExpr =
      Coalesce(
        Seq(
          UnresolvedAttribute("department"),
          UnresolvedAttribute("__auto_generated_subquery_name_l.department")))
    val projectAfterJoin = Project(
      Seq(
        UnresolvedStar(Some(Seq("__auto_generated_subquery_name_s"))),
        Alias(coalesceExpr, "department")()),
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
    val frame = sql(s"source = $sourceTable| LOOKUP $lookupTable uid AS id, name")
    val expectedResults: Array[Row] = Array(
      Row(1000, "Jake", "England", 100000, "IT", "Engineer"),
      Row(1001, "Hello", "USA", 70000, null, null),
      Row(1002, "John", "Canada", 120000, "DATA", "Scientist"),
      Row(1003, "David", null, 120000, "HR", "Doctor"),
      Row(1004, "David", "Canada", 0, null, null),
      Row(1005, "Jane", "Canada", 90000, "DATA", "Engineer"))

    assertSameRows(expectedResults, frame)
  }

  test("test LOOKUP lookupTable name REPLACE occupation") {
    val frame =
      sql(
        s"source = $sourceTable | eval major = occupation | fields id, name, major, country, salary | LOOKUP $lookupTable name REPLACE occupation AS major")
    val expectedResults: Array[Row] = Array(
      Row(1000, "Jake", "England", 100000, "Engineer"),
      Row(1001, "Hello", "USA", 70000, null),
      Row(1002, "John", "Canada", 120000, "Scientist"),
      Row(1003, "David", null, 120000, "Doctor"),
      Row(1004, "David", "Canada", 0, "Doctor"),
      Row(1005, "Jane", "Canada", 90000, "Engineer"))
    assertSameRows(expectedResults, frame)

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
    val projectAfterJoin = Project(
      Seq(
        UnresolvedStar(Some(Seq("__auto_generated_subquery_name_s"))),
        Alias(UnresolvedAttribute("__auto_generated_subquery_name_l.occupation"), "major")()),
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
    val expectedResults: Array[Row] = Array(
      Row(1000, "Jake", "England", 100000, "Engineer"),
      Row(1001, "Hello", "USA", 70000, "Artist"),
      Row(1002, "John", "Canada", 120000, "Doctor"),
      Row(1003, "David", null, 120000, "Doctor"),
      Row(1004, "David", "Canada", 0, "Doctor"),
      Row(1005, "Jane", "Canada", 90000, "Scientist"))
    assertSameRows(expectedResults, frame)

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
      Coalesce(
        Seq(
          UnresolvedAttribute("major"),
          UnresolvedAttribute("__auto_generated_subquery_name_l.occupation")))
    val projectAfterJoin = Project(
      Seq(
        UnresolvedStar(Some(Seq("__auto_generated_subquery_name_s"))),
        Alias(coalesceExpr, "major")()),
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

  test("test LOOKUP lookupTable name") {
    val frame =
      sql(s"source = $sourceTable | LOOKUP $lookupTable name")
    val expectedResults: Array[Row] = Array(
      Row(1000, "Jake", "England", 100000, 1000, "IT", "Engineer"),
      Row(1001, "Hello", "USA", 70000, null, null, null),
      Row(1002, "John", "Canada", 120000, 1002, "DATA", "Scientist"),
      Row(1003, "David", null, 120000, 1003, "HR", "Doctor"),
      Row(1004, "David", "Canada", 0, 1003, "HR", "Doctor"),
      Row(1005, "Jane", "Canada", 90000, 1005, "DATA", "Engineer"))
    assertSameRows(expectedResults, frame)
  }

  // rename country to department for verify the case if search side is not a table
  // and its output has diffed from the original fields of source table
  test("test LOOKUP lookupTable id with rename") {
    val frame =
      sql(
        s"source = $sourceTable | rename id as uid | rename country as department | LOOKUP $lookupTable uid")
    val expectedResults: Array[Row] = Array(
      Row(100000, 1000, "Jake", "IT", "Engineer"),
      Row(70000, 1001, null, null, null),
      Row(120000, 1002, "John", "DATA", "Scientist"),
      Row(120000, 1003, "David", "HR", "Doctor"),
      Row(0, 1004, null, null, null),
      Row(90000, 1005, "Jane", "DATA", "Engineer"))
    assertSameRows(expectedResults, frame)
  }

  test("correctness combination test") {
    sql(s"""
           | CREATE TABLE s
           | (
           |   id INT,
           |   col1 STRING,
           |   col2 STRING
           | )
           | USING $tableType $tableOptions
           |""".stripMargin)
    sql(s"""
           | INSERT INTO s
           | VALUES (1, 'a', 'b'),
           |        (2, 'aa', 'bb'),
           |        (3, null, 'ccc')
           | """.stripMargin)

    sql(s"""
           | CREATE TABLE l
           | (
           |   id INT,
           |   col1 STRING,
           |   col3 STRING
           | )
           | USING $tableType $tableOptions
           |""".stripMargin)
    sql(s"""
           | INSERT INTO l
           | VALUES (1, 'x', 'y'),
           |        (3, 'xx', 'yy')
           | """.stripMargin)
    var frame = sql(s"source = s | LOOKUP l id | fields id, col1, col2, col3")
    var expectedResults =
      Array(Row(1, "x", "b", "y"), Row(2, null, "bb", null), Row(3, "xx", "ccc", "yy"))
    assertSameRows(expectedResults, frame)
    frame = sql(s"source = s | LOOKUP l id REPLACE id, col1, col3 | fields id, col1, col2, col3")
    expectedResults =
      Array(Row(1, "x", "b", "y"), Row(null, null, "bb", null), Row(3, "xx", "ccc", "yy"))
    assertSameRows(expectedResults, frame)
    frame = sql(s"source = s | LOOKUP l id APPEND id, col1, col3 | fields id, col1, col2, col3")
    expectedResults =
      Array(Row(1, "a", "b", "y"), Row(2, "aa", "bb", null), Row(3, "xx", "ccc", "yy"))
    assertSameRows(expectedResults, frame)
    frame = sql(s"source = s | LOOKUP l id REPLACE col1 | fields id, col1, col2")
    expectedResults = Array(Row(1, "x", "b"), Row(2, null, "bb"), Row(3, "xx", "ccc"))
    assertSameRows(expectedResults, frame)
    frame = sql(s"source = s | LOOKUP l id APPEND col1 | fields id, col1, col2")
    expectedResults = Array(Row(1, "a", "b"), Row(2, "aa", "bb"), Row(3, "xx", "ccc"))
    assertSameRows(expectedResults, frame)
    frame = sql(s"source = s | LOOKUP l id REPLACE col1 as col2 | fields id, col1, col2")
    expectedResults = Array(Row(1, "a", "x"), Row(2, "aa", null), Row(3, null, "xx"))
    assertSameRows(expectedResults, frame)
    frame = sql(s"source = s | LOOKUP l id APPEND col1 as col2 | fields id, col1, col2")
    expectedResults = Array(Row(1, "a", "b"), Row(2, "aa", "bb"), Row(3, null, "ccc"))
    assertSameRows(expectedResults, frame)
    frame = sql(s"source = s | LOOKUP l id REPLACE col1 as colA | fields id, col1, col2, colA")
    expectedResults =
      Array(Row(1, "a", "b", "x"), Row(2, "aa", "bb", null), Row(3, null, "ccc", "xx"))
    assertSameRows(expectedResults, frame)
    // source = s | LOOKUP l id APPEND col1 as colA | fields id, col1, col2, colA throw exception
  }

  test("test LOOKUP lookupTable name REPLACE occupation - 2") {
    val frame =
      sql(s"source = $sourceTable | LOOKUP $lookupTable name REPLACE occupation")
    val expectedResults: Array[Row] = Array(
      Row(1000, "Jake", "England", 100000, "Engineer"),
      Row(1001, "Hello", "USA", 70000, null),
      Row(1002, "John", "Canada", 120000, "Scientist"),
      Row(1003, "David", null, 120000, "Doctor"),
      Row(1004, "David", "Canada", 0, "Doctor"),
      Row(1005, "Jane", "Canada", 90000, "Engineer"))
    assertSameRows(expectedResults, frame)
  }

  test("test LOOKUP lookupTable name REPLACE occupation as new_col") {
    val frame =
      sql(s"source = $sourceTable | LOOKUP $lookupTable name REPLACE occupation as new_col")
    val expectedResults: Array[Row] = Array(
      Row(1000, "Jake", "Engineer", "England", 100000, "Engineer"),
      Row(1001, "Hello", "Artist", "USA", 70000, null),
      Row(1002, "John", "Doctor", "Canada", 120000, "Scientist"),
      Row(1003, "David", "Doctor", null, 120000, "Doctor"),
      Row(1004, "David", null, "Canada", 0, "Doctor"),
      Row(1005, "Jane", "Scientist", "Canada", 90000, "Engineer"))
    assertSameRows(expectedResults, frame)
  }

  test("test LOOKUP lookupTable name APPEND occupation as new_col throw exception") {
    val ex = intercept[AnalysisException](sql(s"""
             | source = $sourceTable | LOOKUP $lookupTable name APPEND occupation as new_col
             | """.stripMargin))
    assert(
      ex.getMessage.contains(
        "A column or function parameter with name `new_col` cannot be resolved"))
  }

  test("test LOOKUP lookupTable name as id shouldn't throw exception") {
    sql(s"""
           | DROP TABLE IF EXISTS s
           | """.stripMargin)
    sql(s"""
           | DROP TABLE IF EXISTS l
           | """.stripMargin)
    sql(s"""
           | CREATE TABLE s
           | (
           |   id INT,
           |   uid INT,
           |   name STRING
           | )
           | USING $tableType $tableOptions
           |""".stripMargin)
    sql(s"""
           | INSERT INTO s
           | VALUES (1, 4, 'b'),
           |        (2, 5, 'bb'),
           |        (3, 6, 'ccc')
           | """.stripMargin)

    sql(s"""
           | CREATE TABLE l
           | (
           |   id INT,
           |   name STRING
           | )
           | USING $tableType $tableOptions
           |""".stripMargin)
    sql(s"""
           | INSERT INTO l
           | VALUES (4, 'x'),
           |        (5, 'xx')
           | """.stripMargin)
    val frame =
      sql("source = s | LOOKUP l id as uid REPLACE name")
    val expectedResults: Array[Row] = Array(Row(4, "x"), Row(5, "xx"), Row(6, null))
    assertSameRows(expectedResults, frame)
  }
}
