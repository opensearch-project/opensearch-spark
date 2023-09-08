/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{EqualTo, GreaterThan, LessThanOrEqual, Literal, Not}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project}
import org.apache.spark.sql.streaming.StreamTest

class FlintSparkPPLITSuite
    extends QueryTest
    with FlintPPLSuite
    with StreamTest {

  /** Test table and index name */
  private val testTable = "default.flint_ppl_tst"

  override def beforeAll(): Unit = {
    super.beforeAll()
    
    // Create test table
    sql(s"""
           | CREATE TABLE $testTable
           | (
           |   name STRING,
           |   age INT
           | )
           | USING CSV
           | OPTIONS (
           |  header 'false',
           |  delimiter '\t'
           | )
           | PARTITIONED BY (
           |    year INT,
           |    month INT
           | )
           |""".stripMargin)

    sql(s"""
           | INSERT INTO $testTable
           | PARTITION (year=2023, month=4)
           | VALUES ('Hello', 30)
           | """.stripMargin)
  }

  protected override def afterEach(): Unit = {
    super.afterEach()
    // Stop all streaming jobs if any
    spark.streams.active.foreach { job =>
      job.stop()
      job.awaitTermination()
    }
  }
  
  test("create ppl simple query with start fields result test") {
    val frame = sql(
      s"""
         | source = $testTable
         | """.stripMargin)

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val expectedPlan: LogicalPlan = Project(Seq(UnresolvedStar(None)), UnresolvedRelation(Seq("default","flint_ppl_tst")))
    // Compare the two plans
    assert(expectedPlan === logicalPlan)
  }
  
  test("create ppl simple query two with fields result test") {
    val frame = sql(
      s"""
         | source = $testTable | fields name, age
         | """.stripMargin)

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val expectedPlan: LogicalPlan = Project(Seq(UnresolvedAttribute("name"),UnresolvedAttribute("age")), 
      UnresolvedRelation(Seq("default","flint_ppl_tst")))
    // Compare the two plans
    assert(expectedPlan === logicalPlan)
  }
  
  test("create ppl simple age literal equal filter query with two fields result test") {
    val frame = sql(
      s"""
         | source = $testTable age=25 | fields name, age
         | """.stripMargin)

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val table = UnresolvedRelation(Seq("default","flint_ppl_tst"))
    val filterExpr = EqualTo(UnresolvedAttribute("age"), Literal(25))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedAttribute("name"),UnresolvedAttribute("age"))
    val expectedPlan = Project(projectList, filterPlan)
    // Compare the two plans
    assert(expectedPlan === logicalPlan)
  }
  
  test("create ppl simple age literal greater than filter query with two fields result test") {
    val frame = sql(
      s"""
         | source = $testTable age>25 | fields name, age
         | """.stripMargin)

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val table = UnresolvedRelation(Seq("default","flint_ppl_tst"))
    val filterExpr = GreaterThan(UnresolvedAttribute("age"), Literal(25))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedAttribute("name"),UnresolvedAttribute("age"))
    val expectedPlan = Project(projectList, filterPlan)
    // Compare the two plans
    assert(expectedPlan === logicalPlan)
  }  
  
  test("create ppl simple age literal smaller than equals filter query with two fields result test") {
    val frame = sql(
      s"""
         | source = $testTable age<=65 | fields name, age
         | """.stripMargin)

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val table = UnresolvedRelation(Seq("default","flint_ppl_tst"))
    val filterExpr = LessThanOrEqual(UnresolvedAttribute("age"), Literal(65))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedAttribute("name"),UnresolvedAttribute("age"))
    val expectedPlan = Project(projectList, filterPlan)
    // Compare the two plans
    assert(expectedPlan === logicalPlan)
  }
  
  test("create ppl simple name literal equal filter query with two fields result test") {
    val frame = sql(
      s"""
         | source = $testTable name='George' | fields name, age
         | """.stripMargin)

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val table = UnresolvedRelation(Seq("default","flint_ppl_tst"))
    val filterExpr = EqualTo(UnresolvedAttribute("name"), Literal("'George'"))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedAttribute("name"),UnresolvedAttribute("age"))
    val expectedPlan = Project(projectList, filterPlan)
    // Compare the two plans
    assert(expectedPlan === logicalPlan)
  }  
  
  test("create ppl simple name literal not equal filter query with two fields result test") {
    val frame = sql(
      s"""
         | source = $testTable name!='George' | fields name, age
         | """.stripMargin)

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val table = UnresolvedRelation(Seq("default","flint_ppl_tst"))
    val filterExpr = Not(EqualTo(UnresolvedAttribute("name"), Literal("'George'")))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedAttribute("name"),UnresolvedAttribute("age"))
    val expectedPlan = Project(projectList, filterPlan)
    // Compare the two plans
    assert(expectedPlan === logicalPlan)
  }
}
