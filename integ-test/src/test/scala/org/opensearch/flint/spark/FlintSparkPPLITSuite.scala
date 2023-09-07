/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedRelation}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.streaming.StreamTest

class FlintSparkPPLITSuite
    extends QueryTest
    with FlintPPLSuite
    with StreamTest {

  /** Test table and index name */
  private val testTable = "default.flint_sql_test"

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

  test("create ppl simple query test") {
    val frame = sql(
      s"""
         | source = $testTable
         | """.stripMargin)

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.optimizedPlan
    // Define the expected logical plan
    val expectedPlan: LogicalPlan = Project(Seq(UnresolvedAttribute("*")), UnresolvedRelation(TableIdentifier(testTable)))
    // Compare the two plans
    assert(expectedPlan === logicalPlan)

  }
}
