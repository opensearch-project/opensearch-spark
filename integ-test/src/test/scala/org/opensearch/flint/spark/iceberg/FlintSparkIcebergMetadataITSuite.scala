/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.iceberg

import org.opensearch.flint.spark.covering.FlintSparkCoveringIndex.getFlintIndexName
import org.opensearch.flint.spark.skipping.FlintSparkSkippingIndex.getSkippingIndexName
import org.scalatest.matchers.should.Matchers

import org.apache.spark.sql.{ExplainSuiteHelper, Row}
import org.apache.spark.sql.execution.streaming.{StreamExecution, StreamingQueryWrapper}
import org.apache.spark.sql.flint.config.FlintSparkConf.OPTIMIZER_RULE_COVERING_INDEX_ENABLED

class FlintSparkIcebergMetadataITSuite
    extends FlintSparkIcebergSuite
    with ExplainSuiteHelper
    with Matchers {

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

  /**
   * Logging after V4 Snapshot ID:
   * {"version":1,"snapshot_id":5955838147460350333,"position":1,"scan_all_files":false}
   *
   * Logging after V5 Snapshot ID:
   * {"version":1,"snapshot_id":7393048775934035924,"position":1,"scan_all_files":false}
   *
   * Verify if correct in Iceberg history metadata table
   * |      made_current_at |         snapshot_id |           parent_id | is_current_ancestor |
   * |---------------------:|--------------------:|--------------------:|:--------------------|
   * | 2024-06-17 15:30:... | 1552180198061740807 |                null | true                |
   * | 2024-06-17 15:30:... | 4538411166290920590 | 1552180198061740807 | true                |
   * | 2024-06-17 15:30:... | 8681526725408438495 | 4538411166290920590 | true                |
   * | 2024-06-17 15:30:... | 5955838147460350333 | 8681526725408438495 | true                |
   * | 2024-06-17 15:30:... | 7393048775934035924 | 5955838147460350333 | true                |
   */
  test("version tracking") {
    val job = spark.streams.active.find(_.name == testFlintIndex)
    val query = job.get match {
      case wrapper: StreamingQueryWrapper => wrapper.streamingQuery
      case other: StreamExecution => other
    }

    def snapshotId: String = query.committedOffsets.headOption.get._2.json()

    // v4
    sql(
      s"INSERT INTO $testIcebergTable VALUES (TIMESTAMP '2023-10-04 00:01:00', '200', 'Accept')")
    awaitStreamingComplete(job.get.id.toString)
    logInfo(s"Snapshot ID: $snapshotId")

    // v5
    sql(
      s"INSERT INTO $testIcebergTable VALUES (TIMESTAMP '2023-10-05 00:01:00', '200', 'Accept')")
    awaitStreamingComplete(job.get.id.toString)
    logInfo(s"Snapshot ID: $snapshotId")

    sql(s"SELECT * FROM $testIcebergTable.history").show
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

  // Iceberg relation doesn't support FlinkSparkSkippingFileIndex
  ignore("data compaction impact on skipping index") {
    deleteTestIndex(testFlintIndex)

    sql(s"""
           | CREATE SKIPPING INDEX ON $testIcebergTable (
           |   status_code VALUE_SET
           | )
           | WITH (
           |   auto_refresh = true
           | )
           |""".stripMargin)

    val testFlintSkippingIndex = getSkippingIndexName(testIcebergTable)
    val job = spark.streams.active.find(_.name == testFlintSkippingIndex)
    awaitStreamingComplete(job.get.id.toString)

    // Skipping index works before compaction
    val query = sql(s"SELECT action FROM $testIcebergTable WHERE status_code = '400'")
    checkKeywordsExistsInExplain(query, "FlintSparkSkippingFileIndex")
    checkAnswer(query, Row("Reject"))

    sql(s"""
           | CALL spark_catalog.system.rewrite_data_files (
           |   table => 'covering_sql_iceberg_test',
           |   options => map('rewrite-all', true)
           | )
           | """.stripMargin)

    // Skipping index is invalid afterwards
    awaitStreamingComplete(job.get.id.toString)
    checkAnswer(query, Row("Reject"))
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
