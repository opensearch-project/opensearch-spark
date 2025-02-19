/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedRelation}
import org.apache.spark.sql.catalyst.expressions.{And, Descending, EqualTo, InSubquery, ListQuery, Literal, Not, SortOrder}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Join, JoinHint, LogicalPlan, Project, Sort, SubqueryAlias}
import org.apache.spark.sql.streaming.StreamTest

class FlintSparkPPLInSubqueryITSuite
    extends QueryTest
    with LogicalPlanTestUtils
    with FlintPPLSuite
    with StreamTest {

  /** Test table and index name */
  private val outerTable = "spark_catalog.default.flint_ppl_test1"
  private val innerTable = "spark_catalog.default.flint_ppl_test2"
  private val nestedInnerTable = "spark_catalog.default.flint_ppl_test3"

  override def beforeAll(): Unit = {
    super.beforeAll()
    createPeopleTable(outerTable)
    sql(s"""
           | INSERT INTO $outerTable
           | VALUES (1006, 'Tommy', 'Teacher', 'USA', 30000)
           | """.stripMargin)
    createWorkInformationTable(innerTable)
    createOccupationTable(nestedInnerTable)
  }

  protected override def afterEach(): Unit = {
    super.afterEach()
    // Stop all streaming jobs if any
    spark.streams.active.foreach { job =>
      job.stop()
      job.awaitTermination()
    }
  }

  test("test where id in (select uid from inner)") {
    // id (0, 1, 2, 3, 4, 5, 6), uid (0, 2, 3, 5, 6)
    // InSubquery: (0, 2, 3, 5, 6)
    val frame = sql(s"""
          source = $outerTable
         | | where id in [
         |     source = $innerTable | fields uid
         |   ]
         | | sort  - salary
         | | fields id, name, salary
         | """.stripMargin)
    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] = Array(
      Row(1003, "David", 120000),
      Row(1002, "John", 120000),
      Row(1000, "Jake", 100000),
      Row(1005, "Jane", 90000),
      Row(1006, "Tommy", 30000))
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Integer](_.getAs[Integer](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    val logicalPlan: LogicalPlan = frame.queryExecution.logical

    val outer = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test1"))
    val inner = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test2"))
    val inSubquery =
      Filter(
        InSubquery(
          Seq(UnresolvedAttribute("id")),
          ListQuery(Project(Seq(UnresolvedAttribute("uid")), inner))),
        outer)
    val sortedPlan: LogicalPlan =
      Sort(Seq(SortOrder(UnresolvedAttribute("salary"), Descending)), global = true, inSubquery)
    val expectedPlan =
      Project(
        Seq(
          UnresolvedAttribute("id"),
          UnresolvedAttribute("name"),
          UnresolvedAttribute("salary")),
        sortedPlan)

    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test filter id in (select uid from inner)") {
    val frame = sql(s"""
         source = $outerTable id in [ source = $innerTable | fields uid ]
         | | sort  - salary
         | | fields id, name, salary
         | """.stripMargin)
    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] = Array(
      Row(1003, "David", 120000),
      Row(1002, "John", 120000),
      Row(1000, "Jake", 100000),
      Row(1005, "Jane", 90000),
      Row(1006, "Tommy", 30000))
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Integer](_.getAs[Integer](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    val logicalPlan: LogicalPlan = frame.queryExecution.logical

    val outer = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test1"))
    val inner = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test2"))
    val inSubquery =
      Filter(
        InSubquery(
          Seq(UnresolvedAttribute("id")),
          ListQuery(Project(Seq(UnresolvedAttribute("uid")), inner))),
        outer)
    val sortedPlan: LogicalPlan =
      Sort(Seq(SortOrder(UnresolvedAttribute("salary"), Descending)), global = true, inSubquery)
    val expectedPlan =
      Project(
        Seq(
          UnresolvedAttribute("id"),
          UnresolvedAttribute("name"),
          UnresolvedAttribute("salary")),
        sortedPlan)

    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test where (id) in (select uid from inner)") {
    // id (0, 1, 2, 3, 4, 5, 6), uid (0, 2, 3, 5, 6)
    // InSubquery: (0, 2, 3, 5, 6)
    val frame = sql(s"""
          source = $outerTable
         | | where (id) in [
         |     source = $innerTable | fields uid
         |   ]
         | | sort  - salary
         | | fields id, name, salary
         | """.stripMargin)
    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] = Array(
      Row(1003, "David", 120000),
      Row(1002, "John", 120000),
      Row(1000, "Jake", 100000),
      Row(1005, "Jane", 90000),
      Row(1006, "Tommy", 30000))
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Integer](_.getAs[Integer](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    val logicalPlan: LogicalPlan = frame.queryExecution.logical

    val outer = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test1"))
    val inner = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test2"))
    val inSubquery =
      Filter(
        InSubquery(
          Seq(UnresolvedAttribute("id")),
          ListQuery(Project(Seq(UnresolvedAttribute("uid")), inner))),
        outer)
    val sortedPlan: LogicalPlan =
      Sort(Seq(SortOrder(UnresolvedAttribute("salary"), Descending)), global = true, inSubquery)
    val expectedPlan =
      Project(
        Seq(
          UnresolvedAttribute("id"),
          UnresolvedAttribute("name"),
          UnresolvedAttribute("salary")),
        sortedPlan)

    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test where (id, name) in (select uid, name from inner)") {
    // InSubquery: (0, 2, 3, 5)
    val frame = sql(s"""
          source = $outerTable
         | | where (id, name) in [
         |     source = $innerTable | fields uid, name
         |   ]
         | | sort  - salary
         | | fields id, name, salary
         | """.stripMargin)
    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] = Array(
      Row(1003, "David", 120000),
      Row(1002, "John", 120000),
      Row(1000, "Jake", 100000),
      Row(1005, "Jane", 90000))
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Integer](_.getAs[Integer](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    val logicalPlan: LogicalPlan = frame.queryExecution.logical

    val outer = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test1"))
    val inner = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test2"))
    val inSubquery =
      Filter(
        InSubquery(
          Seq(UnresolvedAttribute("id"), UnresolvedAttribute("name")),
          ListQuery(
            Project(Seq(UnresolvedAttribute("uid"), UnresolvedAttribute("name")), inner))),
        outer)
    val sortedPlan: LogicalPlan =
      Sort(Seq(SortOrder(UnresolvedAttribute("salary"), Descending)), global = true, inSubquery)
    val expectedPlan =
      Project(
        Seq(
          UnresolvedAttribute("id"),
          UnresolvedAttribute("name"),
          UnresolvedAttribute("salary")),
        sortedPlan)

    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test where id not in (select uid from inner)") {
    // id (0, 1, 2, 3, 4, 5, 6), uid (0, 2, 3, 5, 6)
    // Not InSubquery: (1, 4)
    val frame = sql(s"""
          source = $outerTable
         | | where id not in [
         |     source = $innerTable | fields uid
         |   ]
         | | sort  - salary
         | | fields id, name, salary
         | """.stripMargin)
    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] = Array(Row(1001, "Hello", 70000), Row(1004, "David", 0))
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Integer](_.getAs[Integer](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    val logicalPlan: LogicalPlan = frame.queryExecution.logical

    val outer = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test1"))
    val inner = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test2"))
    val inSubquery =
      Filter(
        Not(
          InSubquery(
            Seq(UnresolvedAttribute("id")),
            ListQuery(Project(Seq(UnresolvedAttribute("uid")), inner)))),
        outer)
    val sortedPlan: LogicalPlan =
      Sort(Seq(SortOrder(UnresolvedAttribute("salary"), Descending)), global = true, inSubquery)
    val expectedPlan =
      Project(
        Seq(
          UnresolvedAttribute("id"),
          UnresolvedAttribute("name"),
          UnresolvedAttribute("salary")),
        sortedPlan)

    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test filter id not in (select uid from inner)") {
    val frame = sql(s"""
         source = $outerTable id not in [ source = $innerTable | fields uid ]
         | | sort  - salary
         | | fields id, name, salary
         | """.stripMargin)
    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] = Array(Row(1001, "Hello", 70000), Row(1004, "David", 0))
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Integer](_.getAs[Integer](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    val logicalPlan: LogicalPlan = frame.queryExecution.logical

    val outer = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test1"))
    val inner = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test2"))
    val inSubquery =
      Filter(
        Not(
          InSubquery(
            Seq(UnresolvedAttribute("id")),
            ListQuery(Project(Seq(UnresolvedAttribute("uid")), inner)))),
        outer)
    val sortedPlan: LogicalPlan =
      Sort(Seq(SortOrder(UnresolvedAttribute("salary"), Descending)), global = true, inSubquery)
    val expectedPlan =
      Project(
        Seq(
          UnresolvedAttribute("id"),
          UnresolvedAttribute("name"),
          UnresolvedAttribute("salary")),
        sortedPlan)

    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test where (id, name) not in (select uid, name from inner)") {
    // Not InSubquery: (1, 4, 6)
    val frame = sql(s"""
          source = $outerTable
         | | where (id, name) not in [
         |     source = $innerTable | fields uid, name
         |   ]
         | | sort  - salary
         | | fields id, name, salary
         | """.stripMargin)
    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] =
      Array(Row(1001, "Hello", 70000), Row(1004, "David", 0), Row(1006, "Tommy", 30000))
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Integer](_.getAs[Integer](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    val logicalPlan: LogicalPlan = frame.queryExecution.logical

    val outer = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test1"))
    val inner = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test2"))
    val inSubquery =
      Filter(
        Not(
          InSubquery(
            Seq(UnresolvedAttribute("id"), UnresolvedAttribute("name")),
            ListQuery(
              Project(Seq(UnresolvedAttribute("uid"), UnresolvedAttribute("name")), inner)))),
        outer)
    val sortedPlan: LogicalPlan =
      Sort(Seq(SortOrder(UnresolvedAttribute("salary"), Descending)), global = true, inSubquery)
    val expectedPlan =
      Project(
        Seq(
          UnresolvedAttribute("id"),
          UnresolvedAttribute("name"),
          UnresolvedAttribute("salary")),
        sortedPlan)

    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test empty subquery") {
    // id (0, 1, 2, 3, 4, 5, 6), uid ()
    // InSubquery: ()
    // Not InSubquery: (0, 1, 2, 3, 4, 5, 6)
    var frame = sql(s"""
          source = $outerTable
         | | where id in [
         |     source = $innerTable | where uid = 0000 | fields uid
         |   ]
         | | sort  - salary
         | | fields id, name, salary
         | """.stripMargin)
    var results: Array[Row] = frame.collect()
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Integer](_.getAs[Integer](0))
    var expectedResults: Array[Row] = Array()
    assert(results.sorted.sameElements(expectedResults.sorted))

    frame = sql(s"""
      source = $outerTable
     | | where id not in [
     |     source = $innerTable | where uid = 0000 | fields uid
     |   ]
     | | sort  - salary
     | | fields id, name, salary
     | """.stripMargin)
    results = frame.collect()
    expectedResults = Array(
      Row(1000, "Jake", 100000),
      Row(1001, "Hello", 70000),
      Row(1002, "John", 120000),
      Row(1003, "David", 120000),
      Row(1004, "David", 0),
      Row(1005, "Jane", 90000),
      Row(1006, "Tommy", 30000))
    assert(results.sorted.sameElements(expectedResults.sorted))
  }

  test("test nested subquery") {
    val frame = sql(s"""
          source = $outerTable
         | | where id in [
         |     source = $innerTable
         |     | where occupation in [
         |         source = $nestedInnerTable | where occupation != 'Engineer' | fields occupation
         |       ]
         |     | fields uid
         |   ]
         | | sort  - salary
         | | fields id, name, salary
         | """.stripMargin)
    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] =
      Array(Row(1003, "David", 120000), Row(1002, "John", 120000), Row(1006, "Tommy", 30000))
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Integer](_.getAs[Integer](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    val logicalPlan: LogicalPlan = frame.queryExecution.logical

    val outer = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test1"))
    val inner1 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test2"))
    val inner2 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test3"))
    val filter =
      Filter(Not(EqualTo(UnresolvedAttribute("occupation"), Literal("Engineer"))), inner2)
    val inSubqueryForOccupation =
      Filter(
        InSubquery(
          Seq(UnresolvedAttribute("occupation")),
          ListQuery(Project(Seq(UnresolvedAttribute("occupation")), filter))),
        inner1)
    val inSubqueryForId =
      Filter(
        InSubquery(
          Seq(UnresolvedAttribute("id")),
          ListQuery(Project(Seq(UnresolvedAttribute("uid")), inSubqueryForOccupation))),
        outer)
    val sortedPlan: LogicalPlan =
      Sort(
        Seq(SortOrder(UnresolvedAttribute("salary"), Descending)),
        global = true,
        inSubqueryForId)
    val expectedPlan =
      Project(
        Seq(
          UnresolvedAttribute("id"),
          UnresolvedAttribute("name"),
          UnresolvedAttribute("salary")),
        sortedPlan)

    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test in-subquery as a join filter") {
    val frame = sql(s"""
         | source = $outerTable
         | | inner join left=a, right=b
         |     ON a.id = b.uid AND b.occupation in [
         |         source = $nestedInnerTable| where occupation != 'Engineer' | fields occupation
         |       ]
         |     $innerTable
         | | fields a.id, a.name, a.salary
         | """.stripMargin)
    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] =
      Array(Row(1003, "David", 120000), Row(1002, "John", 120000), Row(1006, "Tommy", 30000))
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Integer](_.getAs[Integer](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    val table1 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test1"))
    val table2 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test2"))
    val inner = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test3"))
    val plan1 = SubqueryAlias("a", table1)
    val plan2 = SubqueryAlias("b", table2)
    val filter =
      Filter(Not(EqualTo(UnresolvedAttribute("occupation"), Literal("Engineer"))), inner)
    val inSubqueryForOccupation =
      InSubquery(
        Seq(UnresolvedAttribute("b.occupation")),
        ListQuery(Project(Seq(UnresolvedAttribute("occupation")), filter)))
    val joinCondition =
      And(
        EqualTo(UnresolvedAttribute("a.id"), UnresolvedAttribute("b.uid")),
        inSubqueryForOccupation)
    val joinPlan = Join(plan1, plan2, Inner, Some(joinCondition), JoinHint.NONE)
    val expectedPlan = Project(
      Seq(
        UnresolvedAttribute("a.id"),
        UnresolvedAttribute("a.name"),
        UnresolvedAttribute("a.salary")),
      joinPlan)
    val logicalPlan: LogicalPlan = frame.queryExecution.logical

    comparePlans(expectedPlan, logicalPlan, checkAnalysis = false)
  }

  test("throw exception because the number of columns not match output of subquery") {
    val ex = intercept[AnalysisException](sql(s"""
          source = $outerTable
         | | where id in [
         |     source = $innerTable | fields uid, department
         |   ]
         | | sort  - salary
         | | fields id, name, salary
         | """.stripMargin))
    assert(ex.getMessage.contains(
      "The number of columns in the left hand side of an IN subquery does not match the number of columns in the output of subquery"))
  }

  test("test in subquery with table alias") {
    val frame = sql(s"""
         | source = $outerTable as o
         | | where id in [
         |   source = $innerTable as i
         | | where i.department = 'DATA'
         | | fields i.uid
         | ]
         | | sort  - o.salary
         | | fields o.id, o.name, o.salary
         | """.stripMargin)
    val expectedResults: Array[Row] = Array(Row(1002, "John", 120000), Row(1005, "Jane", 90000))
    assertSameRows(expectedResults, frame)

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val outer =
      SubqueryAlias("o", UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test1")))
    val inner =
      SubqueryAlias("i", UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test2")))
    val inSubquery =
      Filter(
        InSubquery(
          Seq(UnresolvedAttribute("id")),
          ListQuery(
            Project(
              Seq(UnresolvedAttribute("i.uid")),
              Filter(EqualTo(UnresolvedAttribute("i.department"), Literal("DATA")), inner)))),
        outer)
    val sortedPlan: LogicalPlan =
      Sort(Seq(SortOrder(UnresolvedAttribute("o.salary"), Descending)), global = true, inSubquery)
    val expectedPlan =
      Project(
        Seq(
          UnresolvedAttribute("o.id"),
          UnresolvedAttribute("o.name"),
          UnresolvedAttribute("o.salary")),
        sortedPlan)

    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }
}
