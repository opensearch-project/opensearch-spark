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
import org.opensearch.flint.core.FlintOptions
import org.opensearch.flint.core.storage.FlintOpenSearchIndexMetadataService
import org.opensearch.flint.spark.mv.FlintSparkMaterializedView.getFlintIndexName
import org.scalatest.matchers.must.Matchers.{defined, have}
import org.scalatest.matchers.should.Matchers.{convertToAnyShouldWrapper, the}

import org.apache.spark.sql.Row
import org.apache.spark.sql.flint.FlintDataSourceV2.FLINT_DATASOURCE
import org.apache.spark.sql.flint.config.FlintSparkConf

class FlintSparkMaterializedViewSqlITSuite extends FlintSparkSuite {

  /** Test table, MV, index name and query */
  private val testTable = s"$catalogName.default.mv_test"
  private val testMvName = s"$catalogName.default.mv_test_metrics"
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
    conf.unsetConf(FlintSparkConf.CUSTOM_FLINT_SCHEDULER_CLASS.key)
    conf.unsetConf(FlintSparkConf.EXTERNAL_SCHEDULER_ENABLED.key)
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

  test("create materialized view with auto refresh and external scheduler") {
    withTempDir { checkpointDir =>
      setFlintSparkConf(
        FlintSparkConf.CUSTOM_FLINT_SCHEDULER_CLASS,
        "org.opensearch.flint.spark.scheduler.AsyncQuerySchedulerForSqlIT")
      setFlintSparkConf(FlintSparkConf.EXTERNAL_SCHEDULER_ENABLED, true)

      sql(s"""
           | CREATE MATERIALIZED VIEW $testMvName
           | AS $testQuery
           | WITH (
           |   auto_refresh = true,
           |   scheduler_mode = 'external',
           |   checkpoint_location = '${checkpointDir.getAbsolutePath}',
           |   watermark_delay = '1 Second'
           | )
           | """.stripMargin)

      val indexData = flint.queryIndex(testFlintIndex)

      flint.describeIndex(testFlintIndex) shouldBe defined
      indexData.count() shouldBe 0

      // query index after 25 sec
      Thread.sleep(25000)
      flint.queryIndex(testFlintIndex).count() shouldBe 3

      // New data won't be refreshed until refresh statement triggered
      sql(s"""
           | INSERT INTO $testTable VALUES
           | (TIMESTAMP '2023-10-01 04:00:00', 'F', 25, 'Vancouver')
           | """.stripMargin)
      flint.queryIndex(testFlintIndex).count() shouldBe 3

      // Drop index with test scheduler
      sql(s"DROP MATERIALIZED VIEW $testMvName")
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
    val flintIndexMetadataService =
      new FlintOpenSearchIndexMetadataService(new FlintOptions(openSearchOptions.asJava))

    implicit val formats: Formats = Serialization.formats(NoTypeHints)
    val settings =
      parse(flintIndexMetadataService.getIndexMetadata(testFlintIndex).indexSettings.get)
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

  test("issue 112, https://github.com/opensearch-project/opensearch-spark/issues/112") {
    if (tableType.equalsIgnoreCase("iceberg")) {
      cancel
    }

    val tableName = s"$catalogName.default.issue112"
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
      s""" SELECT
        |   window.start AS `start.time`,
        |   COUNT(*) AS `count`
        | FROM `$catalogName`.`default`.`mv_test`
        | GROUP BY TUMBLE(`time`, '10 Minutes')""".stripMargin.trim

    sql(s"""
           | CREATE MATERIALIZED VIEW `$catalogName`.`default`.`mv_test_metrics`
           | AS $testQuotedQuery
           |""".stripMargin)

    val index = flint.describeIndex(testFlintIndex)
    index shouldBe defined

    val metadata = index.get.metadata()
    metadata.name shouldBe testMvName
    metadata.source shouldBe testQuotedQuery
    metadata.indexedColumns.map(_.asScala("columnName")) shouldBe Seq("start.time", "count")
  }

  Seq(
    s"SELECT name, name FROM $testTable",
    s"SELECT name AS dup_col, age AS dup_col FROM $testTable")
    .foreach { query =>
      test(s"should fail to create materialized view if duplicate columns in $query") {
        the[IllegalArgumentException] thrownBy {
          withTempDir { checkpointDir =>
            sql(s"""
               | CREATE MATERIALIZED VIEW $testMvName
               | AS $query
               | WITH (
               |   auto_refresh = true,
               |   checkpoint_location = '${checkpointDir.getAbsolutePath}'
               | )
               |""".stripMargin)
          }
        } should have message "requirement failed: Duplicate columns found in materialized view query output"
      }
    }

  test("show all materialized views in catalog and database") {
    // Show in catalog
    flint.materializedView().name(s"$catalogName.default.mv1").query(testQuery).create()
    checkAnswer(sql(s"SHOW MATERIALIZED VIEW IN $catalogName"), Seq(Row("mv1")))

    // Show in catalog.database
    flint.materializedView().name(s"$catalogName.default.mv2").query(testQuery).create()
    checkAnswer(
      sql(s"SHOW MATERIALIZED VIEW IN $catalogName.default"),
      Seq(Row("mv1"), Row("mv2")))

    checkAnswer(sql(s"SHOW MATERIALIZED VIEW IN $catalogName.other"), Seq.empty)

    deleteTestIndex(
      getFlintIndexName(s"$catalogName.default.mv1"),
      getFlintIndexName(s"$catalogName.default.mv2"))
  }

  test("show materialized view in database with the same prefix") {
    flint.materializedView().name(s"$catalogName.default.mv1").query(testQuery).create()
    flint.materializedView().name(s"$catalogName.default_test.mv2").query(testQuery).create()
    checkAnswer(sql(s"SHOW MATERIALIZED VIEW IN $catalogName.default"), Seq(Row("mv1")))

    deleteTestIndex(
      getFlintIndexName(s"$catalogName.default.mv1"),
      getFlintIndexName(s"$catalogName.default_test.mv2"))
  }

  test("should return emtpy when show materialized views in empty database") {
    checkAnswer(sql(s"SHOW MATERIALIZED VIEW IN $catalogName.other"), Seq.empty)
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

  test("vacuum materialized view with checkpoint") {
    withTempDir { checkpointDir =>
      flint
        .materializedView()
        .name(testMvName)
        .query(testQuery)
        .options(
          FlintSparkIndexOptions(
            Map(
              "auto_refresh" -> "true",
              "checkpoint_location" -> checkpointDir.getAbsolutePath,
              "watermark_delay" -> "1 Second")),
          testFlintIndex)
        .create()
      flint.refreshIndex(testFlintIndex)

      val job = spark.streams.active.find(_.name == testFlintIndex)
      awaitStreamingComplete(job.get.id.toString)
      flint.deleteIndex(testFlintIndex)

      // Checkpoint folder should be removed after vacuum
      checkpointDir.exists() shouldBe true
      sql(s"VACUUM MATERIALIZED VIEW $testMvName")
      flint.describeIndex(testFlintIndex) shouldBe empty
      checkpointDir.exists() shouldBe false
    }
  }

  test("tumble function should raise error for non-simple time column") {
    val httpLogs = s"$catalogName.default.mv_test_tumble"
    withTable(httpLogs) {
      createTableHttpLog(httpLogs)

      withTempDir { checkpointDir =>
        val ex = the[IllegalStateException] thrownBy {
          sql(s"""
               | CREATE MATERIALIZED VIEW `$catalogName`.`default`.`mv_test_metrics`
               | AS
               | SELECT
               |   window.start AS startTime,
               |   COUNT(*) AS count
               | FROM $httpLogs
               | GROUP BY
               |   TUMBLE(CAST(timestamp AS TIMESTAMP), '10 Minute')
               | WITH (
               |   auto_refresh = true,
               |   checkpoint_location = '${checkpointDir.getAbsolutePath}',
               |   watermark_delay = '1 Second'
               | )
               |""".stripMargin)
        }
        ex.getCause should have message
          "Tumble function only supports simple timestamp column, but found: cast('timestamp as timestamp)"
      }
    }
  }

  test("tumble function should succeed with casted time column within subquery") {
    val httpLogs = s"$catalogName.default.mv_test_tumble"
    withTable(httpLogs) {
      createTableHttpLog(httpLogs)

      withTempDir { checkpointDir =>
        sql(s"""
             | CREATE MATERIALIZED VIEW `$catalogName`.`default`.`mv_test_metrics`
             | AS
             | SELECT
             |   window.start AS startTime,
             |   COUNT(*) AS count
             | FROM (
             |   SELECT CAST(timestamp AS TIMESTAMP) AS time
             |   FROM $httpLogs
             | )
             | GROUP BY
             |   TUMBLE(time, '10 Minute')
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
            Row(timestamp("2023-10-01 10:00:00"), 2),
            Row(timestamp("2023-10-01 10:10:00"), 2)
            /*
             * The last row is pending to fire upon watermark
             *   Row(timestamp("2023-10-01 10:20:00"), 2)
             */
          ))
      }
    }
  }

  private def timestamp(ts: String): Timestamp = Timestamp.valueOf(ts)
}
