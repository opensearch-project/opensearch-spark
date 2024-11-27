/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import java.sql.Date
import java.sql.Timestamp

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.streaming.StreamTest

class FlintSparkPPLCastITSuite
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

  test("test cast number to compatible data types") {
    val frame = sql(s"""
                       | source=$testTable | eval
                       | id_string = cast(id as string),
                       | id_double = cast(id as double),
                       | id_long = cast(id as long),
                       | id_boolean = cast(id as boolean)
                       | | fields id, id_string, id_double, id_long, id_boolean | head 1
                       | """.stripMargin)

    assert(
      frame.dtypes.sameElements(
        Array(
          ("id", "IntegerType"),
          ("id_string", "StringType"),
          ("id_double", "DoubleType"),
          ("id_long", "LongType"),
          ("id_boolean", "BooleanType"))))
    assertSameRows(Seq(Row(1, "1", 1.0, 1L, true)), frame)
  }

  test("test cast string to compatible data types") {
    val frame = sql(s"""
                       | source=$testTable | eval
                       | id_int = cast(cast(id as string) as int),
                       | cast_true = cast("True" as boolean),
                       | cast_false = cast("false" as boolean),
                       | cast_timestamp = cast("2024-11-26 23:39:06" as timestamp),
                       | cast_date = cast("2024-11-26" as date),
                       | cast_time = cast("12:34:56" as time)
                       | | fields id_int, cast_true, cast_false, cast_timestamp, cast_date, cast_time | head 1
                       | """.stripMargin)

    // Note: Spark doesn't support data type of `Time`, cast it to StringTypes by default.
    assert(
      frame.dtypes.sameElements(Array(
        ("id_int", "IntegerType"),
        ("cast_true", "BooleanType"),
        ("cast_false", "BooleanType"),
        ("cast_timestamp", "TimestampType"),
        ("cast_date", "DateType"),
        ("cast_time", "StringType"))))
    assertSameRows(
      Seq(
        Row(
          1,
          true,
          false,
          Timestamp.valueOf("2024-11-26 23:39:06"),
          Date.valueOf("2024-11-26"),
          "12:34:56")),
      frame)
  }

  test("test cast time related types to compatible data types") {
    val frame = sql(s"""
                       | source=$testTable | eval
                       | timestamp = cast("2024-11-26 23:39:06" as timestamp),
                       | ts_str = cast(timestamp as string),
                       | ts_date = cast(timestamp as date),
                       | date_str = cast(ts_date as string),
                       | date_ts = cast(ts_date as timestamp)
                       | | fields timestamp, ts_str, ts_date, date_str, date_ts | head 1
                       | """.stripMargin)

    assert(
      frame.dtypes.sameElements(
        Array(
          ("timestamp", "TimestampType"),
          ("ts_str", "StringType"),
          ("ts_date", "DateType"),
          ("date_str", "StringType"),
          ("date_ts", "TimestampType"))))
    assertSameRows(
      Seq(
        Row(
          Timestamp.valueOf("2024-11-26 23:39:06"),
          "2024-11-26 23:39:06",
          Date.valueOf("2024-11-26"),
          "2024-11-26",
          Timestamp.valueOf("2024-11-26 00:00:00"))),
      frame)
  }

}
