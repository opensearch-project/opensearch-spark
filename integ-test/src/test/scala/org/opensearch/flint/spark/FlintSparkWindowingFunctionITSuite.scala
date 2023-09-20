/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import java.sql.Timestamp

import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import org.apache.spark.FlintSuite
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.types.StructType

class FlintSparkWindowingFunctionITSuite extends QueryTest with FlintSuite {

  test("tumble windowing function") {
    val inputDF = spark
      .createDataFrame(
        Seq(
          (1L, "2023-01-01 00:00:00"),
          (2L, "2023-01-01 00:09:00"),
          (3L, "2023-01-01 00:15:00")))
      .toDF("id", "timestamp")

    val resultDF = inputDF.selectExpr("TUMBLE(timestamp, '10 minutes')")

    resultDF.schema shouldBe StructType.fromDDL(
      "window struct<start:timestamp,end:timestamp> NOT NULL")
    checkAnswer(
      resultDF,
      Seq(
        Row(Row(timestamp("2023-01-01 00:00:00"), timestamp("2023-01-01 00:10:00"))),
        Row(Row(timestamp("2023-01-01 00:00:00"), timestamp("2023-01-01 00:10:00"))),
        Row(Row(timestamp("2023-01-01 00:10:00"), timestamp("2023-01-01 00:20:00")))))
  }

  private def timestamp(ts: String): Timestamp = Timestamp.valueOf(ts)
}
