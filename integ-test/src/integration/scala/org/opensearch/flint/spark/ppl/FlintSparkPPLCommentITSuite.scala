/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import org.opensearch.sql.ppl.utils.DataTypeTransformer.seq

import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Ascending, CaseWhen, Descending, EqualTo, GreaterThanOrEqual, LessThan, Literal, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.streaming.StreamTest

class FlintSparkPPLCommentITSuite
    extends QueryTest
    with LogicalPlanTestUtils
    with FlintPPLSuite
    with StreamTest {

  private val testTable = "spark_catalog.default.flint_ppl_test"

  override def beforeAll(): Unit = {
    super.beforeAll()

    createPartitionedStateCountryTable(testTable)
  }

  protected override def afterEach(): Unit = {
    super.afterEach()
    spark.streams.active.foreach { job =>
      job.stop()
      job.awaitTermination()
    }
  }

  test("test line comment") {
    val frame = sql(s"""
                       | /*
                       |  * This is a
                       |  *   multiple
                       |  * line block
                       |  *   comment
                       |  */
                       | source = /* block comment */ $testTable /* block comment */
                       | | eval /*
                       |        This is a
                       |          multiple
                       |        line
                       |          block
                       |            comment
                       |        */ col = 1
                       | | /* block comment */ fields name, /* block comment */ age
                       | /* block comment */
                       | """.stripMargin)

    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] =
      Array(Row("Jake", 70), Row("Hello", 30), Row("John", 25), Row("Jane", 20))
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))
  }

  test("test block comment") {
    val frame = sql(s"""
                       | source = $testTable //line comment
                       | | eval col = 1 // line comment
                       | | fields name, age // line comment
                       | /////////line comment
                       | """.stripMargin)

    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] =
      Array(Row("Jake", 70), Row("Hello", 30), Row("John", 25), Row("Jane", 20))
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))
  }
}
