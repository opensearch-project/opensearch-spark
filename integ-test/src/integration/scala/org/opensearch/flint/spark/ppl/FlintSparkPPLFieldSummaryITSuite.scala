/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.flint.spark.ppl

import org.opensearch.sql.ppl.utils.DataTypeTransformer.seq

import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{Alias, Ascending, Expression, Literal, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.streaming.StreamTest

class FlintSparkPPLFieldSummaryITSuite
    extends QueryTest
    with LogicalPlanTestUtils
    with FlintPPLSuite
    with StreamTest {

  private val testTable = "spark_catalog.default.flint_ppl_test"

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Create test table
    createNullableTableHttpLog(testTable)
  }

  protected override def afterEach(): Unit = {
    super.afterEach()
    // Stop all streaming jobs if any
    spark.streams.active.foreach { job =>
      job.stop()
      job.awaitTermination()
    }
  }

  ignore("test fieldsummary with single field includefields(status_code) & nulls=true ") {
    val frame = sql(s"""
                       | source = $testTable | fieldsummary includefields= status_code nulls=true
                       | """.stripMargin)

    /*
        val frame = sql(s"""
                           | SELECT
                           |     'status_code' AS Field,
                           |     COUNT(status_code) AS Count,
                           |     COUNT(DISTINCT status_code) AS Distinct,
                           |     MIN(status_code) AS Min,
                           |     MAX(status_code) AS Max,
                           |     AVG(CAST(status_code AS DOUBLE)) AS Avg,
                           |     typeof(status_code) AS Type,
                           |     COUNT(*) - COUNT(status_code) AS Nulls
                           | FROM $testTable
                           | GROUP BY typeof(status_code)
                           | """.stripMargin)
     */

//    val frame = sql(s"""
//                       | SELECT
//                       |     'status_code' AS Field,
//                       |     COUNT(status_code) AS Count,
//                       |     COUNT(DISTINCT status_code) AS Distinct,
//                       |     MIN(status_code) AS Min,
//                       |     MAX(status_code) AS Max,
//                       |     AVG(CAST(status_code AS DOUBLE)) AS Avg,
//                       |     typeof(status_code) AS Type,
//                       |     (SELECT COLLECT_LIST(STRUCT(status_code, count_status))
//                       |      FROM (
//                       |         SELECT status_code, COUNT(*) AS count_status
//                       |         FROM $testTable
//                       |         GROUP BY status_code
//                       |         ORDER BY count_status DESC
//                       |         LIMIT 5
//                       |     )) AS top_values,
//                       |     COUNT(*) - COUNT(status_code) AS Nulls
//                       | FROM $testTable
//                       | GROUP BY typeof(status_code)
//                       |
//                       | UNION ALL
//                       |
//                       | SELECT
//                       |     'id' AS Field,
//                       |     COUNT(id) AS Count,
//                       |     COUNT(DISTINCT id) AS Distinct,
//                       |     MIN(id) AS Min,
//                       |     MAX(id) AS Max,
//                       |     AVG(CAST(id AS DOUBLE)) AS Avg,
//                       |     typeof(id) AS Type,
//                       |     (SELECT COLLECT_LIST(STRUCT(id, count_id))
//                       |      FROM (
//                       |         SELECT id, COUNT(*) AS count_id
//                       |         FROM $testTable
//                       |         GROUP BY id
//                       |         ORDER BY count_id DESC
//                       |         LIMIT 5
//                       |     )) AS top_values,
//                       |     COUNT(*) - COUNT(id) AS Nulls
//                       | FROM $testTable
//                       | GROUP BY typeof(id)
//                       |""".stripMargin)

    val results: Array[Row] = frame.collect()
    // Print each row in a readable format
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // scalastyle:off println
    results.foreach(row => println(row.mkString(", ")))
    println(logicalPlan)
    // scalastyle:on println

//    val expectedPlan = ?
//    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

}
