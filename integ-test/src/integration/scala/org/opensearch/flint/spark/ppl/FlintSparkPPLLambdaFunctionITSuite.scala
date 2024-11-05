/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.functions.{col, to_json}
import org.apache.spark.sql.streaming.StreamTest

class FlintSparkPPLLambdaFunctionITSuite
    extends QueryTest
    with LogicalPlanTestUtils
    with FlintPPLSuite
    with StreamTest {

  /** Test table and index name */
  private val testTable = "spark_catalog.default.flint_ppl_test"

  override def beforeAll(): Unit = {
    super.beforeAll()
    // Create test table
    createNullableJsonContentTable(testTable)
  }

  protected override def afterEach(): Unit = {
    super.afterEach()
    // Stop all streaming jobs if any
    spark.streams.active.foreach { job =>
      job.stop()
      job.awaitTermination()
    }
  }

  test("test forall()") {
    val frame = sql(s"""
                       | source = $testTable | eval array = json_array(1,2,0,-1,1.1,-0.11), result = forall(array, x -> x > 0) | head 1 | fields result
                       | """.stripMargin)
    assertSameRows(Seq(Row(false)), frame)

    val frame2 = sql(s"""
                        | source = $testTable | eval array = json_array(1,2,0,-1,1.1,-0.11), result = forall(array, x -> x > -10) | head 1 | fields result
                        | """.stripMargin)
    assertSameRows(Seq(Row(true)), frame2)

    val frame3 = sql(s"""
                        | source = $testTable | eval array = json_array(json_object("a",1,"b",-1),json_object("a",-1,"b",-1)), result = forall(array, x -> x.a > 0) | head 1 | fields result
                        | """.stripMargin)
    assertSameRows(Seq(Row(false)), frame3)

    val frame4 = sql(s"""
                        | source = $testTable | eval array = json_array(json_object("a",1,"b",-1),json_object("a",-1,"b",-1)), result = exists(array, x -> x.b < 0) | head 1 | fields result
                        | """.stripMargin)
    assertSameRows(Seq(Row(true)), frame4)
  }

  test("test exists()") {
    val frame = sql(s"""
                       | source = $testTable | eval array = json_array(1,2,0,-1,1.1,-0.11), result = exists(array, x -> x > 0) | head 1 | fields result
                       | """.stripMargin)
    assertSameRows(Seq(Row(true)), frame)

    val frame2 = sql(s"""
                        | source = $testTable | eval array = json_array(1,2,0,-1,1.1,-0.11), result = exists(array, x -> x > 10) | head 1 | fields result
                        | """.stripMargin)
    assertSameRows(Seq(Row(false)), frame2)

    val frame3 = sql(s"""
                        | source = $testTable | eval array = json_array(json_object("a",1,"b",-1),json_object("a",-1,"b",-1)), result = exists(array, x -> x.a > 0) | head 1 | fields result
                        | """.stripMargin)
    assertSameRows(Seq(Row(true)), frame3)

    val frame4 = sql(s"""
                        | source = $testTable | eval array = json_array(json_object("a",1,"b",-1),json_object("a",-1,"b",-1)), result = exists(array, x -> x.b > 0) | head 1 | fields result
                        | """.stripMargin)
    assertSameRows(Seq(Row(false)), frame4)

  }

  test("test filter()") {
    val frame = sql(s"""
         | source = $testTable| eval array = json_array(1,2,0,-1,1.1,-0.11), result = filter(array, x -> x > 0) | head 1 | fields result
         | """.stripMargin)
    assertSameRows(Seq(Row(Seq(1, 2, 1.1))), frame)

    val frame2 = sql(s"""
         | source = $testTable| eval array = json_array(1,2,0,-1,1.1,-0.11), result = filter(array, x -> x > 10) | head 1 | fields result
         | """.stripMargin)
    assertSameRows(Seq(Row(Seq())), frame2)

    val frame3 = sql(s"""
         | source = $testTable| eval array = json_array(json_object("a",1,"b",-1),json_object("a",-1,"b",-1)), result = filter(array, x -> x.a > 0) | head 1 | fields result
         | """.stripMargin)

    assertSameRows(Seq(Row("""[{"a":1,"b":-1}]""")), frame3.select(to_json(col("result"))))

    val frame4 = sql(s"""
         | source = $testTable| eval array = json_array(json_object("a",1,"b",-1),json_object("a",-1,"b",-1)), result = filter(array, x -> x.b > 0) | head 1 | fields result
         | """.stripMargin)
    assertSameRows(Seq(Row("""[]""")), frame4.select(to_json(col("result"))))
  }

  test("test transform()") {
    val frame = sql(s"""
                       | source = $testTable | eval array = json_array(1,2,3), result = transform(array, x -> x + 1) | head 1 | fields result
                       | """.stripMargin)
    assertSameRows(Seq(Row(Seq(2, 3, 4))), frame)

    val frame2 = sql(s"""
                        | source = $testTable | eval array = json_array(1,2,3), result = transform(array, (x, y) -> x + y) | head 1 | fields result
                        | """.stripMargin)
    assertSameRows(Seq(Row(Seq(1, 3, 5))), frame2)
  }

  test("test reduce()") {
    val frame = sql(s"""
                       | source = $testTable | eval array = json_array(1,2,3), result = reduce(array, 0, (acc, x) -> acc + x) | head 1 | fields result
                       | """.stripMargin)
    assertSameRows(Seq(Row(6)), frame)

    val frame2 = sql(s"""
                       | source = $testTable | eval array = json_array(1,2,3), result = reduce(array, 1, (acc, x) -> acc + x) | head 1 | fields result
                       | """.stripMargin)
    assertSameRows(Seq(Row(7)), frame2)

    val frame3 = sql(s"""
                        | source = $testTable | eval array = json_array(1,2,3), result = reduce(array, 0, (acc, x) -> acc + x, acc -> acc * 10) | head 1 | fields result
                        | """.stripMargin)
    assertSameRows(Seq(Row(60)), frame3)
  }
}
