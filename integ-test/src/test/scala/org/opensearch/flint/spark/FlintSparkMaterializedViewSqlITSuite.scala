/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import java.sql.Timestamp

import scala.Option.empty
import scala.collection.JavaConverters.{mapAsJavaMapConverter, mapAsScalaMapConverter}

import org.json4s.{Formats, NoTypeHints}
import org.json4s.native.JsonMethods.parse
import org.json4s.native.Serialization
import org.opensearch.client.RequestOptions
import org.opensearch.client.indices.GetIndexRequest
import org.opensearch.flint.core.FlintOptions
import org.opensearch.flint.core.storage.FlintOpenSearchClient
import org.opensearch.flint.spark.mv.FlintSparkMaterializedView.getFlintIndexName
import org.scalatest.matchers.must.Matchers.{defined, have}
import org.scalatest.matchers.should.Matchers.{convertToAnyShouldWrapper, the}

import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.flint.FlintDataSourceV2.FLINT_DATASOURCE

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

  override def beforeEach(): Unit = {
    super.beforeAll()
    createTimeSeriesTable(testTable)
  }

  override def afterEach(): Unit = {
    super.afterEach()
    deleteTestIndex(testFlintIndex)
    sql(s"DROP TABLE $testTable")
  }

  test("create materialized view with auto refresh") {
    withTempDir { checkpointDir =>
      sql(s"""
           | CREATE MATERIALIZED VIEW $testMvName
           | AS $testQuery
           | WITH (
           |   auto_refresh = true,
           |   checkpoint_location = '${checkpointDir.getAbsolutePath}',
           |   watermark_delay = '1 Second'
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
             |   watermark_delay = '1 Second',
             |   output_mode = 'complete',
             |   index_settings = '{"number_of_shards": 3, "number_of_replicas": 2}',
             |   extra_options = '{"$testTable": {"maxFilesPerTrigger": "1"}}'
             | )
             |""".stripMargin)

      val index = flint.describeIndex(testFlintIndex)
      index shouldBe defined

      val options = index.get.options
      options.autoRefresh() shouldBe true
      options.refreshInterval() shouldBe Some("5 Seconds")
      options.checkpointLocation() shouldBe Some(checkpointDir.getAbsolutePath)
      options.watermarkDelay() shouldBe Some("1 Second")
      options.outputMode() shouldBe Some("complete")
      options.extraSourceOptions(testTable) shouldBe Map("maxFilesPerTrigger" -> "1")
      options.extraSinkOptions() shouldBe Map.empty
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

  test("create materialized view with full refresh") {
    sql(s"""
         | CREATE MATERIALIZED VIEW $testMvName
         | AS $testQuery
         | WITH (
         |   auto_refresh = false
         | )
         |""".stripMargin)

    val indexData = spark.read.format(FLINT_DATASOURCE).load(testFlintIndex)
    flint.describeIndex(testFlintIndex) shouldBe defined
    indexData.count() shouldBe 0

    sql(s"REFRESH MATERIALIZED VIEW $testMvName")
    indexData.count() shouldBe 4
  }

  test("create materialized view with incremental refresh") {
    withTempDir { checkpointDir =>
      sql(s"""
             | CREATE MATERIALIZED VIEW $testMvName
             | AS $testQuery
             | WITH (
             |   incremental_refresh = true,
             |   checkpoint_location = '${checkpointDir.getAbsolutePath}',
             |   watermark_delay = '1 Second'
             | )
             | """.stripMargin)

      // Refresh all present source data as of now
      sql(s"REFRESH MATERIALIZED VIEW $testMvName")
      flint.queryIndex(testFlintIndex).count() shouldBe 3

      // New data won't be refreshed until refresh statement triggered
      sql(s"""
           | INSERT INTO $testTable VALUES
           | (TIMESTAMP '2023-10-01 04:00:00', 'F', 25, 'Vancouver')
           | """.stripMargin)
      flint.queryIndex(testFlintIndex).count() shouldBe 3

      // New data is refreshed incrementally
      sql(s"REFRESH MATERIALIZED VIEW $testMvName")
      flint.queryIndex(testFlintIndex).count() shouldBe 4
    }
  }

  test("create materialized view if not exists") {
    sql(s"CREATE MATERIALIZED VIEW IF NOT EXISTS $testMvName AS $testQuery")
    flint.describeIndex(testFlintIndex) shouldBe defined

    // Expect error without IF NOT EXISTS, otherwise success
    the[IllegalStateException] thrownBy
      sql(s"CREATE MATERIALIZED VIEW $testMvName AS $testQuery")

    sql(s"CREATE MATERIALIZED VIEW IF NOT EXISTS $testMvName AS $testQuery")
  }

  test("should fail if materialized view name is too long") {
    the[IllegalArgumentException] thrownBy {
      val testMvLongName = "x" * 255
      sql(s"CREATE MATERIALIZED VIEW $testMvLongName AS $testQuery")
    } should have message
      "requirement failed: Flint index name exceeds the maximum allowed length of 255 characters"
  }

  test("should fail if materialized view query has syntax error") {
    the[ParseException] thrownBy {
      // Wrong syntax due to incomplete WITH clause
      sql(s"""
           | CREATE MATERIALIZED VIEW $testMvName
           | AS
           | SELECT time FROM $testTable WITH (
           | """.stripMargin)
    }
  }

  test("should fail if materialized view query has semantic error") {
    the[AnalysisException] thrownBy {
      // Non-existent time1 column
      sql(s"""
           | CREATE MATERIALIZED VIEW $testMvName
           | AS
           | SELECT time1 FROM $testTable
           | """.stripMargin)
    }
  }

  test("should fail if no windowing function for aggregated query") {
    withTempDir { checkpointDir =>
      the[IllegalArgumentException] thrownBy {
        sql(s"""
            | CREATE MATERIALIZED VIEW $testMvName
            | AS
            | SELECT COUNT(*) FROM $testTable GROUP BY time
            | WITH (
            |   auto_refresh = true,
            |   checkpoint_location = '${checkpointDir.getAbsolutePath}',
            |   watermark_delay = '1 Second'
            | )
            | """.stripMargin)
      } should have message
        "requirement failed: A windowing function is required for auto or incremental refresh with aggregation"

      // OS index should not be created because of pre-validation failed above
      openSearchClient
        .indices()
        .exists(new GetIndexRequest(testFlintIndex), RequestOptions.DEFAULT) shouldBe false
    }
  }

  test("should fail if no watermark delay for aggregated query") {
    withTempDir { checkpointDir =>
      the[IllegalArgumentException] thrownBy {
        sql(s"""
               | CREATE MATERIALIZED VIEW $testMvName
               | AS
               | $testQuery
               | WITH (
               |   auto_refresh = true,
               |   checkpoint_location = '${checkpointDir.getAbsolutePath}'
               | )
               | """.stripMargin)
      } should have message
        "requirement failed: watermark delay is required for auto or incremental refresh with aggregation"

      // OS index should not be created because of pre-validation failed above
      openSearchClient
        .indices()
        .exists(new GetIndexRequest(testFlintIndex), RequestOptions.DEFAULT) shouldBe false
    }
  }

  test("issue 112, https://github.com/opensearch-project/opensearch-spark/issues/112") {
    val tableName = "spark_catalog.default.issue112"
    createTableIssue112(tableName)
    sql(s"""
           |CREATE MATERIALIZED VIEW $testMvName AS
           |    SELECT
           |    rs.resource.attributes.key as resource_key,
           |    rs.resource.attributes.value.stringValue as resource_value,
           |    ss.scope.name as scope_name,
           |    ss.scope.version as scope_version,
           |    span.attributes.key as span_key,
           |    span.attributes.value.intValue as span_int_value,
           |    span.attributes.value.stringValue as span_string_value,
           |    span.endTimeUnixNano,
           |    span.kind,
           |    span.name as span_name,
           |    span.parentSpanId,
           |    span.spanId,
           |    span.startTimeUnixNano,
           |    span.traceId
           |    FROM $tableName
           |    LATERAL VIEW
           |    EXPLODE(resourceSpans) as rs
           |    LATERAL VIEW
           |    EXPLODE(rs.scopeSpans) as ss
           |    LATERAL VIEW
           |    EXPLODE(ss.spans) as span
           |    LATERAL VIEW
           |    EXPLODE(span.attributes) as span_attr
           |WITH (
           |  auto_refresh = false
           |)
           """.stripMargin)

    val indexData = spark.read.format(FLINT_DATASOURCE).load(testFlintIndex)
    flint.describeIndex(testFlintIndex) shouldBe defined
    indexData.count() shouldBe 0

    sql(s"REFRESH MATERIALIZED VIEW $testMvName")
    indexData.count() shouldBe 2
  }

  test("create materialized view with quoted name and column name") {
    val testQuotedQuery =
      """ SELECT
        |   window.start AS `start.time`,
        |   COUNT(*) AS `count`
        | FROM `spark_catalog`.`default`.`mv_test`
        | GROUP BY TUMBLE(`time`, '10 Minutes')""".stripMargin.trim

    sql(s"""
           | CREATE MATERIALIZED VIEW `spark_catalog`.`default`.`mv_test_metrics`
           | AS $testQuotedQuery
           |""".stripMargin)

    val index = flint.describeIndex(testFlintIndex)
    index shouldBe defined

    val metadata = index.get.metadata()
    metadata.name shouldBe testMvName
    metadata.source shouldBe testQuotedQuery
    metadata.indexedColumns.map(_.asScala("columnName")) shouldBe Seq("start.time", "count")
  }

  test("show all materialized views in catalog and database") {
    // Show in catalog
    flint.materializedView().name("spark_catalog.default.mv1").query(testQuery).create()
    checkAnswer(sql(s"SHOW MATERIALIZED VIEW IN spark_catalog"), Seq(Row("mv1")))

    // Show in catalog.database
    flint.materializedView().name("spark_catalog.default.mv2").query(testQuery).create()
    checkAnswer(
      sql(s"SHOW MATERIALIZED VIEW IN spark_catalog.default"),
      Seq(Row("mv1"), Row("mv2")))

    checkAnswer(sql(s"SHOW MATERIALIZED VIEW IN spark_catalog.other"), Seq.empty)
  }

  test("should return emtpy when show materialized views in empty database") {
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
      Seq(Row("startTime", "timestamp"), Row("count", "bigint")))
  }

  test("should return empty when describe nonexistent materialized view") {
    checkAnswer(sql("DESC MATERIALIZED VIEW nonexistent_mv"), Seq())
  }

  test("update materialized view") {
    withTempDir { checkpointDir =>
      flint
        .materializedView()
        .name(testMvName)
        .query(testQuery)
        .create()

      flint.describeIndex(testFlintIndex) shouldBe defined
      flint.queryIndex(testFlintIndex).count() shouldBe 0

      sql(s"""
           | ALTER MATERIALIZED VIEW $testMvName
           | WITH (
           |   auto_refresh = true,
           |   checkpoint_location = '${checkpointDir.getAbsolutePath}',
           |   watermark_delay = '1 Second'
           | )
           |""".stripMargin)

      // Wait for streaming job complete current micro batch
      val job = spark.streams.active.find(_.name == testFlintIndex)
      job shouldBe defined
      failAfter(streamingTimeout) {
        job.get.processAllAvailable()
      }

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

  test("drop and vacuum materialized view") {
    flint
      .materializedView()
      .name(testMvName)
      .query(testQuery)
      .create()

    sql(s"DROP MATERIALIZED VIEW $testMvName")
    sql(s"VACUUM MATERIALIZED VIEW $testMvName")
    flint.describeIndex(testFlintIndex) shouldBe empty
  }

  private def timestamp(ts: String): Timestamp = Timestamp.valueOf(ts)
}
