/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedRelation}
import org.apache.spark.sql.catalyst.expressions.{And, IsNotNull, IsNull, Or}
import org.apache.spark.sql.catalyst.plans.logical.{Deduplicate, Filter, LogicalPlan, Project, Union}
import org.apache.spark.sql.streaming.StreamTest

class FlintSparkPPLDedupeITSuite
    extends QueryTest
    with LogicalPlanTestUtils
    with FlintPPLSuite
    with StreamTest {

  /** Test table and index name */
  private val testTable = "spark_catalog.default.flint_ppl_test"

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Create test table
    createDuplicationNullableTable(testTable)
  }

  protected override def afterEach(): Unit = {
    super.afterEach()
    // Stop all streaming jobs if any
    spark.streams.active.foreach { job =>
      job.stop()
      job.awaitTermination()
    }
  }

  test("test dedupe 1 name") {
    val frame = sql(s"""
         | source = $testTable | dedup 1 name | fields name
         | """.stripMargin)

    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] = Array(Row("A"), Row("B"), Row("C"), Row("D"), Row("E"))
    implicit val oneColRowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val fieldsProjectList = Seq(UnresolvedAttribute("name"))
    val dedupKeys = Seq(UnresolvedAttribute("name"))
    val filter = Filter(IsNotNull(UnresolvedAttribute("name")), table)
    val expectedPlan = Project(fieldsProjectList, Deduplicate(dedupKeys, filter))
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test dedupe 1 name, category") {
    val frame = sql(s"""
         | source = $testTable | dedup 1 name, category | fields name, category
         | """.stripMargin)

    val results: Array[Row] = frame.collect()
    // results.foreach(println(_))
    val expectedResults: Array[Row] = Array(
      Row("A", "X"),
      Row("A", "Y"),
      Row("B", "Z"),
      Row("C", "X"),
      Row("D", "Z"),
      Row("B", "Y"))
    implicit val twoColsRowOrdering: Ordering[Row] =
      Ordering.by[Row, (String, String)](row => (row.getAs(0), row.getAs(1)))
    assert(results.sorted.sameElements(expectedResults.sorted))

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val fieldsProjectList = Seq(UnresolvedAttribute("name"), UnresolvedAttribute("category"))
    val dedupKeys = Seq(UnresolvedAttribute("name"), UnresolvedAttribute("category"))
    val filter = Filter(
      And(IsNotNull(UnresolvedAttribute("name")), IsNotNull(UnresolvedAttribute("category"))),
      table)
    val expectedPlan = Project(fieldsProjectList, Deduplicate(dedupKeys, filter))
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test dedupe 1 name KEEPEMPTY=true") {
    val frame = sql(s"""
         | source = $testTable | dedup 1 name KEEPEMPTY=true | fields name, category
         | """.stripMargin)

    val results: Array[Row] = frame.collect()
    // results.foreach(println(_))
    val expectedResults: Array[Row] = Array(
      Row("A", "X"),
      Row("B", "Z"),
      Row("C", "X"),
      Row("D", "Z"),
      Row("E", null),
      Row(null, "Y"),
      Row(null, "X"),
      Row(null, "Z"),
      Row(null, null))
    implicit val nullableTwoColsRowOrdering: Ordering[Row] =
      Ordering.by[Row, (String, String)](row => {
        val value0 = row.getAs[String](0)
        val value1 = row.getAs[String](1)
        (
          if (value0 == null) String.valueOf(Int.MaxValue) else value0,
          if (value1 == null) String.valueOf(Int.MaxValue) else value1)
      })
    assert(
      results.sorted
        .map(_.getAs[String](0))
        .sameElements(expectedResults.sorted.map(_.getAs[String](0))))

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val fieldsProjectList = Seq(UnresolvedAttribute("name"), UnresolvedAttribute("category"))
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val isNotNullFilter =
      Filter(IsNotNull(UnresolvedAttribute("name")), table)
    val deduplicate = Deduplicate(Seq(UnresolvedAttribute("name")), isNotNullFilter)
    val isNullFilter = Filter(IsNull(UnresolvedAttribute("name")), table)
    val union = Union(deduplicate, isNullFilter)
    val expectedPlan = Project(fieldsProjectList, union)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test dedupe 1 name, category KEEPEMPTY=true") {
    val frame = sql(s"""
         | source = $testTable | dedup 1 name, category KEEPEMPTY=true | fields name, category
         | """.stripMargin)

    val results: Array[Row] = frame.collect()
    // results.foreach(println(_))
    val expectedResults: Array[Row] = Array(
      Row("A", "X"),
      Row("A", "Y"),
      Row("B", "Z"),
      Row("C", "X"),
      Row("D", "Z"),
      Row("B", "Y"),
      Row(null, "Y"),
      Row("E", null),
      Row(null, "X"),
      Row("B", null),
      Row(null, "Z"),
      Row(null, null))
    implicit val nullableTwoColsRowOrdering: Ordering[Row] =
      Ordering.by[Row, (String, String)](row => {
        val value0 = row.getAs[String](0)
        val value1 = row.getAs[String](1)
        (
          if (value0 == null) String.valueOf(Int.MaxValue) else value0,
          if (value1 == null) String.valueOf(Int.MaxValue) else value1)
      })
    assert(results.sorted.sameElements(expectedResults.sorted))

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val fieldsProjectList = Seq(UnresolvedAttribute("name"), UnresolvedAttribute("category"))
    val isNotNullFilter = Filter(
      And(IsNotNull(UnresolvedAttribute("name")), IsNotNull(UnresolvedAttribute("category"))),
      table)
    val deduplicate = Deduplicate(
      Seq(UnresolvedAttribute("name"), UnresolvedAttribute("category")),
      isNotNullFilter)
    val isNullFilter = Filter(
      Or(IsNull(UnresolvedAttribute("name")), IsNull(UnresolvedAttribute("category"))),
      table)
    val union = Union(deduplicate, isNullFilter)
    val expectedPlan = Project(fieldsProjectList, union)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test 1 name CONSECUTIVE=true") {
    val ex = intercept[UnsupportedOperationException](sql(s"""
             | source = $testTable | dedup 1 name CONSECUTIVE=true | fields name
             | """.stripMargin))
    assert(ex.getMessage.contains("Consecutive deduplication is not supported"))
  }

  test("test 1 name KEEPEMPTY=true CONSECUTIVE=true") {
    val ex = intercept[UnsupportedOperationException](sql(s"""
             | source = $testTable | dedup 1 name KEEPEMPTY=true CONSECUTIVE=true | fields name
             | """.stripMargin))
    assert(ex.getMessage.contains("Consecutive deduplication is not supported"))
  }

  test("test dedupe 2 name") {
    val frame = sql(s"""
         | source = $testTable| dedup 2 name | fields name
         | """.stripMargin)

    val results: Array[Row] = frame.collect()
    // results.foreach(println(_))
    val expectedResults: Array[Row] =
      Array(Row("A"), Row("A"), Row("B"), Row("B"), Row("C"), Row("C"), Row("D"), Row("E"))
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))
  }

  test("test dedupe 2 name, category") {
    val frame = sql(s"""
         | source = $testTable| dedup 2 name, category | fields name, category
         | """.stripMargin)

    val results: Array[Row] = frame.collect()
    // results.foreach(println(_))
    val expectedResults: Array[Row] = Array(
      Row("A", "X"),
      Row("A", "X"),
      Row("A", "Y"),
      Row("A", "Y"),
      Row("B", "Y"),
      Row("B", "Z"),
      Row("B", "Z"),
      Row("C", "X"),
      Row("C", "X"),
      Row("D", "Z"))
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](row => {
      val value = row.getAs[String](0)
      if (value == null) String.valueOf(Int.MaxValue) else value
    })
    assert(results.sorted.sameElements(expectedResults.sorted))
  }

  test("test dedupe 2 name KEEPEMPTY=true") {
    val frame = sql(s"""
         | source = $testTable| dedup 2 name KEEPEMPTY=true | fields name, category
         | """.stripMargin)

    val results: Array[Row] = frame.collect()
    // results.foreach(println(_))
    val expectedResults: Array[Row] = Array(
      Row("A", "X"),
      Row("A", "Y"),
      Row("B", "Z"),
      Row("B", "Z"),
      Row("C", "X"),
      Row("C", "X"),
      Row("D", "Z"),
      Row("E", null),
      Row(null, "Y"),
      Row(null, "X"),
      Row(null, "Z"),
      Row(null, null))
    implicit val nullableTwoColsRowOrdering: Ordering[Row] =
      Ordering.by[Row, (String, String)](row => {
        val value0 = row.getAs[String](0)
        val value1 = row.getAs[String](1)
        (
          if (value0 == null) String.valueOf(Int.MaxValue) else value0,
          if (value1 == null) String.valueOf(Int.MaxValue) else value1)
      })
    assert(
      results.sorted
        .map(_.getAs[String](0))
        .sameElements(expectedResults.sorted.map(_.getAs[String](0))))
  }

  test("test dedupe 2 name, category KEEPEMPTY=true") {
    val frame = sql(s"""
         | source = $testTable| dedup 2 name, category KEEPEMPTY=true | fields name, category
         | """.stripMargin)

    val results: Array[Row] = frame.collect()
    // results.foreach(println(_))
    val expectedResults: Array[Row] = Array(
      Row("A", "X"),
      Row("A", "X"),
      Row("A", "Y"),
      Row("A", "Y"),
      Row("B", "Y"),
      Row("B", "Z"),
      Row("B", "Z"),
      Row("C", "X"),
      Row("C", "X"),
      Row("D", "Z"),
      Row(null, "Y"),
      Row("E", null),
      Row(null, "X"),
      Row("B", null),
      Row(null, "Z"),
      Row(null, null))
    implicit val nullableTwoColsRowOrdering: Ordering[Row] =
      Ordering.by[Row, (String, String)](row => {
        val value0 = row.getAs[String](0)
        val value1 = row.getAs[String](1)
        (
          if (value0 == null) String.valueOf(Int.MaxValue) else value0,
          if (value1 == null) String.valueOf(Int.MaxValue) else value1)
      })
    assert(results.sorted.sameElements(expectedResults.sorted))
  }

  test("test 2 name CONSECUTIVE=true") {
    val ex = intercept[UnsupportedOperationException](sql(s"""
             | source = $testTable | dedup 2 name CONSECUTIVE=true | fields name
             | """.stripMargin))
    assert(ex.getMessage.contains("Consecutive deduplication is not supported"))
  }

  test("test 2 name KEEPEMPTY=true CONSECUTIVE=true") {
    val ex = intercept[UnsupportedOperationException](sql(s"""
             | source = $testTable | dedup 2 name KEEPEMPTY=true CONSECUTIVE=true | fields name
             | """.stripMargin))
    assert(ex.getMessage.contains("Consecutive deduplication is not supported"))
  }
}
