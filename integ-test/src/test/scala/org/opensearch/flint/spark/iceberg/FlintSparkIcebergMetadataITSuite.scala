/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.iceberg

import org.opensearch.flint.spark.covering.FlintSparkCoveringIndex.getFlintIndexName
import org.scalatest.matchers.should.Matchers

import org.apache.spark.sql.flint.config.FlintSparkConf.OPTIMIZER_RULE_COVERING_INDEX_ENABLED

class FlintSparkIcebergMetadataITSuite extends FlintSparkIcebergSuite with Matchers {

  private val testIcebergTable = "spark_catalog.default.covering_sql_iceberg_test"
  private val testFlintIndex = getFlintIndexName("all", testIcebergTable)

  override def beforeAll(): Unit = {
    super.beforeAll()
    setFlintSparkConf(OPTIMIZER_RULE_COVERING_INDEX_ENABLED, "false")
  }

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    sql(s"""
           | CREATE TABLE $testIcebergTable (
           |   time_dt TIMESTAMP,
           |   status_code STRING,
           |   action STRING
           | )
           | USING iceberg
           |""".stripMargin)

    // v1
    sql(
      s"INSERT INTO $testIcebergTable VALUES (TIMESTAMP '2023-10-01 00:01:00', '200', 'Accept')")
    // v2
    sql(
      s"INSERT INTO $testIcebergTable VALUES (TIMESTAMP '2023-10-02 00:01:00', '200', 'Accept')")
    // v3
    sql(
      s"INSERT INTO $testIcebergTable VALUES (TIMESTAMP '2023-10-03 00:01:00', '400', 'Reject')")

    sql(s"""
           | CREATE INDEX all ON $testIcebergTable (
           |   time_dt, status_code, action
           | )
           | WITH (
           |   auto_refresh = true
           | )
           |""".stripMargin)
    val job = spark.streams.active.find(_.name == testFlintIndex)
    awaitStreamingComplete(job.get.id.toString)
  }

  test("data expiration") {
    flint.queryIndex(testFlintIndex).count() shouldBe 3

    sql(s"""
           | CALL spark_catalog.system.expire_snapshots (
           |   table => 'covering_sql_iceberg_test',
           |   older_than => 1718222788758
           | )
           | """.stripMargin)

    val job = spark.streams.active.find(_.name == testFlintIndex)
    awaitStreamingComplete(job.get.id.toString)
    flint.queryIndex(testFlintIndex).count() shouldBe 3
  }

  test("data compaction") {
    flint.queryIndex(testFlintIndex).count() shouldBe 3

    sql(s"""
           | CALL spark_catalog.system.rewrite_data_files (
           |   table => 'covering_sql_iceberg_test',
           |   options => map('rewrite-all', true)
           | )
           | """.stripMargin)

    // A new empty micro batch is generated
    val job = spark.streams.active.find(_.name == testFlintIndex)
    awaitStreamingComplete(job.get.id.toString)
    flint.queryIndex(testFlintIndex).count() shouldBe 3
  }

  test("schema evolution") {
    flint.queryIndex(testFlintIndex).count() shouldBe 3

    sql(s"""
           | ALTER TABLE $testIcebergTable
           | ADD COLUMN severity_id INT
           | """.stripMargin)

    // No new micro batch after schema changed (no new snapshot)
    val job = spark.streams.active.find(_.name == testFlintIndex)
    awaitStreamingComplete(job.get.id.toString)
    flint.queryIndex(testFlintIndex).count() shouldBe 3

    // v4 with new column
    sql(
      s"INSERT INTO $testIcebergTable VALUES (TIMESTAMP '2023-10-04 00:01:00', '304', 'Accept', 3)")
    awaitStreamingComplete(job.get.id.toString)
    flint.queryIndex(testFlintIndex).count() shouldBe 4
    flint.queryIndex(testFlintIndex).schema.fields.length shouldBe 3
  }
}
