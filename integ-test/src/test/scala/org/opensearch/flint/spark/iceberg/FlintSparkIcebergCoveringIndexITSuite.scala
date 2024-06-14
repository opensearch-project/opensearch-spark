/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.iceberg

import org.opensearch.flint.spark.FlintSparkCoveringIndexSqlITSuite
import org.opensearch.flint.spark.covering.FlintSparkCoveringIndex.getFlintIndexName
import org.scalatest.matchers.should.Matchers

class FlintSparkIcebergCoveringIndexITSuite
    extends FlintSparkCoveringIndexSqlITSuite
    with FlintSparkIcebergSuite
    with Matchers {

  private val testIcebergTable = "spark_catalog.default.covering_sql_iceberg_test"

  test("create covering index on Iceberg struct type") {
    sql(s"""
        | CREATE TABLE $testIcebergTable (
        |   status_code STRING,
        |   src_endpoint STRUCT<ip: STRING, port: INT>
        | )
        | USING iceberg
        |""".stripMargin)
    sql(s"INSERT INTO $testIcebergTable VALUES ('200', STRUCT('192.168.0.1', 80))")

    val testFlintIndex = getFlintIndexName("all", testIcebergTable)
    sql(s"""
         | CREATE INDEX all ON $testIcebergTable (
         |   status_code, src_endpoint
         | )
         | WITH (
         |   auto_refresh = true
         | )
         |""".stripMargin)

    val job = spark.streams.active.find(_.name == testFlintIndex)
    awaitStreamingComplete(job.get.id.toString)

    flint.queryIndex(testFlintIndex).count() shouldBe 1
  }
}
