/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import java.sql.Timestamp

import scala.Option.empty
import scala.collection.JavaConverters.mapAsJavaMapConverter

import org.json4s.{Formats, NoTypeHints}
import org.json4s.native.JsonMethods.parse
import org.json4s.native.Serialization
import org.opensearch.flint.core.FlintOptions
import org.opensearch.flint.core.storage.FlintOpenSearchClient
import org.opensearch.flint.spark.mv.FlintSparkMaterializedView.getFlintIndexName
import org.scalatest.matchers.must.Matchers.defined
import org.scalatest.matchers.should.Matchers.{convertToAnyShouldWrapper, the}

import org.apache.spark.sql.Row

class FlintSparkMaterializedViewSqlITSuite extends FlintSparkSuite {

  /** Test table, MV, index name and query */
  private val testTable = "spark_catalog.default.mv_test"
  private val testMvName = "spark_catalog.default.mv_test_metrics"
  private val testFlintIndex = getFlintIndexName(testMvName)
  private val testQuery =
    s"""
       | SELECT
       |   window.start AS startTime,
       |   COUNT(*) AS count
       | FROM $testTable
       | GROUP BY TUMBLE(time, '10 Minutes')
       |""".stripMargin

  override def beforeAll(): Unit = {
    super.beforeAll()
    createTimeSeriesTable(testTable)
  }

  override def afterEach(): Unit = {
    super.afterEach()
    flint.deleteIndex(testFlintIndex)
  }

  test("create materialized view with auto refresh") {
    withTempDir { checkpointDir =>
      sql(s"""
           | CREATE MATERIALIZED VIEW $testMvName
           | AS $testQuery
           | WITH (
           |   auto_refresh = true,
           |   checkpoint_location = '${checkpointDir.getAbsolutePath}'
           | )
           |""".stripMargin)

      // Wait for streaming job complete current micro batch
      val job = spark.streams.active.find(_.name == testFlintIndex)
      job shouldBe defined
      failAfter(streamingTimeout) {
        job.get.processAllAvailable()
      }

      flint.describeIndex(testFlintIndex) shouldBe defined
      checkAnswer(
        flint.queryIndex(testFlintIndex).select("startTime", "count"),
        Seq(
          Row(timestamp("2023-10-01 00:00:00"), 1),
          Row(timestamp("2023-10-01 00:10:00"), 2),
          Row(timestamp("2023-10-01 01:00:00"), 1)
          /*
           * The last row is pending to fire upon watermark
           *   Row(timestamp("2023-10-01 02:00:00"), 1)
           */
        ))
    }
  }

  test("create materialized view with streaming job options") {
    withTempDir { checkpointDir =>
      sql(s"""
             | CREATE MATERIALIZED VIEW $testMvName
             | AS $testQuery
             | WITH (
             |   auto_refresh = true,
             |   refresh_interval = '5 Seconds',
             |   checkpoint_location = '${checkpointDir.getAbsolutePath}',
             |   index_settings = '{"number_of_shards": 3, "number_of_replicas": 2}'
             | )
             |""".stripMargin)

      val index = flint.describeIndex(testFlintIndex)
      index shouldBe defined
      index.get.options.autoRefresh() shouldBe true
      index.get.options.refreshInterval() shouldBe Some("5 Seconds")
      index.get.options.checkpointLocation() shouldBe Some(checkpointDir.getAbsolutePath)
    }
  }

  test("create materialized view with index settings") {
    sql(s"""
             | CREATE MATERIALIZED VIEW $testMvName
             | AS $testQuery
             | WITH (
             |   index_settings = '{"number_of_shards": 3, "number_of_replicas": 2}'
             | )
             |""".stripMargin)

    // Check if the index setting option is set to OS index setting
    val flintClient = new FlintOpenSearchClient(new FlintOptions(openSearchOptions.asJava))

    implicit val formats: Formats = Serialization.formats(NoTypeHints)
    val settings = parse(flintClient.getIndexMetadata(testFlintIndex).indexSettings.get)
    (settings \ "index.number_of_shards").extract[String] shouldBe "3"
    (settings \ "index.number_of_replicas").extract[String] shouldBe "2"
  }

  test("create materialized view if not exists") {
    sql(s"CREATE MATERIALIZED VIEW IF NOT EXISTS $testMvName AS $testQuery")
    flint.describeIndex(testFlintIndex) shouldBe defined

    // Expect error without IF NOT EXISTS, otherwise success
    the[IllegalStateException] thrownBy
      sql(s"CREATE MATERIALIZED VIEW $testMvName AS $testQuery")

    sql(s"CREATE MATERIALIZED VIEW IF NOT EXISTS $testMvName AS $testQuery")
  }

  test("show all materialized views in catalog") {
    flint.materializedView().name("spark_catalog.default.mv1").query(testQuery).create()
    checkAnswer(
      sql(s"SHOW MATERIALIZED VIEW IN spark_catalog"),
      Seq(Row("spark_catalog.default.mv1")))

    flint.materializedView().name("spark_catalog.default.mv2").query(testQuery).create()
    checkAnswer(
      sql(s"SHOW MATERIALIZED VIEW IN spark_catalog"),
      Seq(Row("spark_catalog.default.mv1"), Row("spark_catalog.default.mv2")))

    checkAnswer(sql(s"SHOW MATERIALIZED VIEW IN spark_catalog.other"), Seq.empty)
  }

  test("describe materialized view") {
    flint
      .materializedView()
      .name(testMvName)
      .query(testQuery)
      .create()

    checkAnswer(
      sql(s"DESC MATERIALIZED VIEW $testMvName"),
      Seq(Row("startTime", "timestamp"), Row("count", "long")))
  }

  test("drop materialized view") {
    flint
      .materializedView()
      .name(testMvName)
      .query(testQuery)
      .create()

    sql(s"DROP MATERIALIZED VIEW $testMvName")

    flint.describeIndex(testFlintIndex) shouldBe empty
  }

  private def timestamp(ts: String): Timestamp = Timestamp.valueOf(ts)
}
