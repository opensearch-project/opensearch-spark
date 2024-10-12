/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Ascending, Descending, EqualTo, Exists, GreaterThan, InSubquery, ListQuery, Literal, Not, Or, ScalarSubquery, SortOrder}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.streaming.StreamTest

class FlintSparkPPLExistsSubqueryITSuite
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

  test("test simple exists subquery") {
    val frame = sql(s"""
                       | source = $outerTable
                       | | where exists [
                       |     source = $innerTable | where id = uid
                       |   ]
                       | | sort  - salary
                       | | fields id, name, salary
                       | """.stripMargin)
    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] = Array(
      Row(1002, "John", 120000),
      Row(1003, "David", 120000),
      Row(1000, "Jake", 100000),
      Row(1005, "Jane", 90000),
      Row(1006, "Tommy", 30000))
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Integer](_.getAs[Integer](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    val logicalPlan: LogicalPlan = frame.queryExecution.logical

    val outer = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test1"))
    val inner = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test2"))
    val existsSubquery = Filter(
      Exists(Filter(EqualTo(UnresolvedAttribute("id"), UnresolvedAttribute("uid")), inner)),
      outer)
    val sortedPlan = Sort(
      Seq(SortOrder(UnresolvedAttribute("salary"), Descending)),
      global = true,
      existsSubquery)
    val expectedPlan =
      Project(
        Seq(
          UnresolvedAttribute("id"),
          UnresolvedAttribute("name"),
          UnresolvedAttribute("salary")),
        sortedPlan)

    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test not exists subquery") {
    val frame = sql(s"""
                       | source = $outerTable
                       | | where not exists [
                       |     source = $innerTable | where id = uid
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
    val existsSubquery =
      Filter(
        Not(
          Exists(Filter(EqualTo(UnresolvedAttribute("id"), UnresolvedAttribute("uid")), inner))),
        outer)
    val sortedPlan = Sort(
      Seq(SortOrder(UnresolvedAttribute("salary"), Descending)),
      global = true,
      existsSubquery)
    val expectedPlan =
      Project(
        Seq(
          UnresolvedAttribute("id"),
          UnresolvedAttribute("name"),
          UnresolvedAttribute("salary")),
        sortedPlan)

    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test empty exists subquery") {
    var frame = sql(s"""
                       | source = $outerTable
                       | | where exists [
                       |     source = $innerTable | where uid = 0000 AND id = uid
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
                   | | where not exists [
                   |     source = $innerTable | where uid = 0000 AND id = uid
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

  test("test uncorrelated exists subquery") {
    var frame = sql(s"""
                       | source = $outerTable
                       | | where exists [
                       |     source = $innerTable | where like(name, 'J%')
                       |   ]
                       | | sort  - salary
                       | | fields id, name, salary
                       | """.stripMargin)
    val results: Array[Row] = frame.collect()
    assert(results.length == 7)

    val logicalPlan: LogicalPlan = frame.queryExecution.logical

    val outer = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test1"))
    val inner = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test2"))
    val existsSubquery =
      Filter(
        Exists(
          Filter(
            UnresolvedFunction(
              "like",
              Seq(UnresolvedAttribute("name"), Literal("J%")),
              isDistinct = false),
            inner)),
        outer)
    val sortedPlan = Sort(
      Seq(SortOrder(UnresolvedAttribute("salary"), Descending)),
      global = true,
      existsSubquery)
    val expectedPlan =
      Project(
        Seq(
          UnresolvedAttribute("id"),
          UnresolvedAttribute("name"),
          UnresolvedAttribute("salary")),
        sortedPlan)

    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)

    frame = sql(s"""
                   | source = $outerTable
                   | | where not exists [
                   |     source = $innerTable | where like(name, 'J%')
                   |   ]
                   | | sort  - salary
                   | | fields id, name, salary
                   | """.stripMargin)
    assert(frame.collect().length == 0)

    frame = sql(s"""
                   | source = $outerTable
                   | | where exists [
                   |     source = $innerTable | where like(name, 'X%')
                   |   ]
                   | | sort  - salary
                   | | fields id, name, salary
                   | """.stripMargin)
    assert(frame.collect().length == 0)
  }

  test("uncorrelated exists subquery check the return content of inner table is empty or not") {
    var frame = sql(s"""
                       | source = $outerTable
                       | | where exists [
                       |     source = $innerTable
                       |   ]
                       | | eval constant = "Bala"
                       | | fields constant
                       | """.stripMargin)
    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] = Array(
      Row("Bala"),
      Row("Bala"),
      Row("Bala"),
      Row("Bala"),
      Row("Bala"),
      Row("Bala"),
      Row("Bala"))
    assert(results.sameElements(expectedResults))

    val logicalPlan: LogicalPlan = frame.queryExecution.logical

    val outer = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test1"))
    val inner = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test2"))
    val existsSubquery = Filter(Exists(inner), outer)
    val evalProject =
      Project(Seq(UnresolvedStar(None), Alias(Literal("Bala"), "constant")()), existsSubquery)
    val expectedPlan = Project(Seq(UnresolvedAttribute("constant")), evalProject)

    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)

    frame = sql(s"""
                   | source = $outerTable
                   | | where exists [
                   |     source = $innerTable | where uid = 999
                   |   ]
                   | | eval constant = "Bala"
                   | | fields constant
                   | """.stripMargin)
    frame.show
    assert(frame.collect().length == 0)
  }

  test("test nested exists subquery") {
    val frame = sql(s"""
                       | source = $outerTable
                       | | where exists [
                       |     source = $innerTable
                       |     | where exists [
                       |         source = $nestedInnerTable
                       |         | where $nestedInnerTable.occupation = $innerTable.occupation
                       |       ]
                       |     | where id = uid
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
    val inner1 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test2"))
    val inner2 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test3"))
    val existsSubqueryForOccupation =
      Filter(
        Exists(
          Filter(
            EqualTo(
              UnresolvedAttribute("spark_catalog.default.flint_ppl_test3.occupation"),
              UnresolvedAttribute("spark_catalog.default.flint_ppl_test2.occupation")),
            inner2)),
        inner1)
    val existsSubqueryForId =
      Filter(
        Exists(
          Filter(
            EqualTo(UnresolvedAttribute("id"), UnresolvedAttribute("uid")),
            existsSubqueryForOccupation)),
        outer)
    val sortedPlan: LogicalPlan =
      Sort(
        Seq(SortOrder(UnresolvedAttribute("salary"), Descending)),
        global = true,
        existsSubqueryForId)
    val expectedPlan =
      Project(
        Seq(
          UnresolvedAttribute("id"),
          UnresolvedAttribute("name"),
          UnresolvedAttribute("salary")),
        sortedPlan)

    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test exists subquery with conjunction of conditions") {
    val frame = sql(s"""
                       | source = $outerTable
                       | | where exists [
                       |     source = $innerTable
                       |     | where id = uid AND
                       |       $outerTable.name = $innerTable.name AND
                       |       $outerTable.occupation = $innerTable.occupation
                       |   ]
                       | | sort  - salary
                       | | fields id, name, salary
                       | """.stripMargin)
    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] = Array(Row(1003, "David", 120000), Row(1000, "Jake", 100000))
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Integer](_.getAs[Integer](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    val logicalPlan: LogicalPlan = frame.queryExecution.logical

    val outer = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test1"))
    val inner = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test2"))
    val existsSubquery = Filter(
      Exists(
        Filter(
          And(
            And(
              EqualTo(UnresolvedAttribute("id"), UnresolvedAttribute("uid")),
              EqualTo(
                UnresolvedAttribute("spark_catalog.default.flint_ppl_test1.name"),
                UnresolvedAttribute("spark_catalog.default.flint_ppl_test2.name"))),
            EqualTo(
              UnresolvedAttribute("spark_catalog.default.flint_ppl_test1.occupation"),
              UnresolvedAttribute("spark_catalog.default.flint_ppl_test2.occupation"))),
          inner)),
      outer)
    val sortedPlan = Sort(
      Seq(SortOrder(UnresolvedAttribute("salary"), Descending)),
      global = true,
      existsSubquery)
    val expectedPlan =
      Project(
        Seq(
          UnresolvedAttribute("id"),
          UnresolvedAttribute("name"),
          UnresolvedAttribute("salary")),
        sortedPlan)

    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }
}
