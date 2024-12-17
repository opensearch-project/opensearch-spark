/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import scala.Option.empty
import scala.collection.JavaConverters.{mapAsJavaMapConverter, mapAsScalaMapConverter}

import org.json4s.{Formats, NoTypeHints}
import org.json4s.native.JsonMethods.parse
import org.json4s.native.Serialization
import org.opensearch.flint.core.FlintOptions
import org.opensearch.flint.core.storage.FlintOpenSearchIndexMetadataService
import org.opensearch.flint.spark.covering.FlintSparkCoveringIndex.getFlintIndexName
import org.opensearch.flint.spark.skipping.FlintSparkSkippingIndex.getSkippingIndexName
import org.scalatest.matchers.must.Matchers.defined
import org.scalatest.matchers.should.Matchers.{convertToAnyShouldWrapper, the}

import org.apache.spark.sql.Row
import org.apache.spark.sql.flint.config.FlintSparkConf
import org.apache.spark.sql.flint.config.FlintSparkConf.{CHECKPOINT_MANDATORY, OPTIMIZER_RULE_COVERING_INDEX_ENABLED}

class FlintSparkCoveringIndexSqlITSuite extends FlintSparkSuite {

  /** Test table and index name */
  private val testTable = "spark_catalog.default.covering_sql_test"
  private val testIndex = "name_and_age"
  private val testFlintIndex = getFlintIndexName(testIndex, testTable)
  private val testTimeSeriesTable = "spark_catalog.default.covering_sql_ts_test"
  private val testFlintTimeSeriesIndex = getFlintIndexName(testIndex, testTimeSeriesTable)

  override def beforeEach(): Unit = {
    super.beforeEach()

    createPartitionedAddressTable(testTable)
    createTimeSeriesTable(testTimeSeriesTable)
  }

  override def afterEach(): Unit = {
    super.afterEach()

    // Delete all test indices
    deleteTestIndex(testFlintIndex, testFlintTimeSeriesIndex)
    sql(s"DROP TABLE $testTable")
    sql(s"DROP TABLE $testTimeSeriesTable")
  }

  test("create covering index with auto refresh") {
    awaitRefreshComplete(s"""
           | CREATE INDEX $testIndex ON $testTable
           | (name, age)
           | WITH (auto_refresh = true)
           | """.stripMargin)

    val indexData = flint.queryIndex(testFlintIndex)
    indexData.count() shouldBe 2
  }

  test("create covering index with filtering condition") {
    awaitRefreshComplete(s"""
           | CREATE INDEX $testIndex ON $testTable
           | (name, age)
           | WHERE address = 'Portland'
           | WITH (auto_refresh = true)
           |""".stripMargin)

    val indexData = flint.queryIndex(testFlintIndex)
    indexData.count() shouldBe 1
  }

  test("create covering index with streaming job options") {
    withTempDir { checkpointDir =>
      sql(s"""
             | CREATE INDEX $testIndex ON $testTable ( name )
             | WITH (
             |   auto_refresh = true,
             |   refresh_interval = '5 Seconds',
             |   checkpoint_location = '${checkpointDir.getAbsolutePath}',
             |   extra_options = '{"$testTable": {"maxFilesPerTrigger": "1"}}'
             | )
             | """.stripMargin)

      val index = flint.describeIndex(testFlintIndex)
      index shouldBe defined
      index.get.options.autoRefresh() shouldBe true
      index.get.options.refreshInterval() shouldBe Some("5 Seconds")
      index.get.options.checkpointLocation() shouldBe Some(checkpointDir.getAbsolutePath)
    }
  }

  test("create covering index with auto refresh and ID expression") {
    sql(s"""
           | CREATE INDEX $testIndex ON $testTimeSeriesTable
           | (time, age, address)
           | WITH (
           |   auto_refresh = true,
           |   id_expression = 'address'
           | )
           |""".stripMargin)

    val job = spark.streams.active.find(_.name == testFlintTimeSeriesIndex)
    awaitStreamingComplete(job.get.id.toString)

    val indexData = flint.queryIndex(testFlintTimeSeriesIndex)
    indexData.count() shouldBe 3 // only 3 rows left due to same ID
  }

  test("create covering index with full refresh and ID expression") {
    sql(s"""
           | CREATE INDEX $testIndex ON $testTimeSeriesTable
           | (time, age, address)
           | WITH (
           |   id_expression = 'address'
           | )
           |""".stripMargin)
    sql(s"REFRESH INDEX $testIndex ON $testTimeSeriesTable")

    val indexData = flint.queryIndex(testFlintTimeSeriesIndex)
    indexData.count() shouldBe 3 // only 3 rows left due to same ID

    // Rerun should not generate duplicate data
    sql(s"REFRESH INDEX $testIndex ON $testTimeSeriesTable")
    indexData.count() shouldBe 3
  }

  test("create covering index with index settings") {
    sql(s"""
           | CREATE INDEX $testIndex ON $testTable ( name )
           | WITH (
           |   index_settings = '{"number_of_shards": 2, "number_of_replicas": 3}'
           | )
           |""".stripMargin)

    // Check if the index setting option is set to OS index setting
    val flintIndexMetadataService =
      new FlintOpenSearchIndexMetadataService(new FlintOptions(openSearchOptions.asJava))

    implicit val formats: Formats = Serialization.formats(NoTypeHints)
    val settings =
      parse(flintIndexMetadataService.getIndexMetadata(testFlintIndex).indexSettings.get)
    (settings \ "index.number_of_shards").extract[String] shouldBe "2"
    (settings \ "index.number_of_replicas").extract[String] shouldBe "3"
  }

  test("create covering index with invalid option") {
    the[IllegalArgumentException] thrownBy
      sql(s"""
             | CREATE INDEX $testIndex ON $testTable
             | (name, age)
             | WITH (autoRefresh = true)
             | """.stripMargin)
  }

  test("create skipping index with auto refresh should fail if mandatory checkpoint enabled") {
    setFlintSparkConf(CHECKPOINT_MANDATORY, "true")
    try {
      the[IllegalArgumentException] thrownBy {
        sql(s"""
               | CREATE INDEX $testIndex ON $testTable
               | (name, age)
               | WITH (auto_refresh = true)
               | """.stripMargin)
      }
    } finally {
      setFlintSparkConf(CHECKPOINT_MANDATORY, "false")
    }
  }

  test("create covering index with full refresh") {
    sql(s"""
           | CREATE INDEX $testIndex ON $testTable
           | (name, age)
           |""".stripMargin)

    val indexData = flint.queryIndex(testFlintIndex)

    flint.describeIndex(testFlintIndex) shouldBe defined
    indexData.count() shouldBe 0

    sql(s"REFRESH INDEX $testIndex ON $testTable")
    indexData.count() shouldBe 2
  }

  test("create covering index with external scheduler") {
    withTempDir { checkpointDir =>
      setFlintSparkConf(
        FlintSparkConf.CUSTOM_FLINT_SCHEDULER_CLASS,
        "org.opensearch.flint.spark.scheduler.AsyncQuerySchedulerForSqlIT")
      setFlintSparkConf(FlintSparkConf.EXTERNAL_SCHEDULER_ENABLED, true)

      sql(s"""
           | CREATE INDEX $testIndex ON $testTable
           | (name, age)
           | WITH (
           |   auto_refresh = true,
           |   scheduler_mode = 'external',
           |   checkpoint_location = '${checkpointDir.getAbsolutePath}'
           | )
           | """.stripMargin)

      val indexData = flint.queryIndex(testFlintIndex)

      flint.describeIndex(testFlintIndex) shouldBe defined
      indexData.count() shouldBe 0

      // query index after 25 sec
      Thread.sleep(25000)
      flint.queryIndex(testFlintIndex).count() shouldBe 2

      // New data won't be refreshed until refresh statement triggered
      sql(s"""
           | INSERT INTO $testTable
           | PARTITION (year=2023, month=5)
           | VALUES ('Hello', 50, 'Vancouver')
           |""".stripMargin)
      flint.queryIndex(testFlintIndex).count() shouldBe 2

      // Drop index with test scheduler
      sql(s"DROP INDEX $testIndex ON $testTable")
      conf.unsetConf(FlintSparkConf.CUSTOM_FLINT_SCHEDULER_CLASS.key)
      conf.unsetConf(FlintSparkConf.EXTERNAL_SCHEDULER_ENABLED.key)
    }
  }

  test("create covering index with incremental refresh") {
    withTempDir { checkpointDir =>
      sql(s"""
             | CREATE INDEX $testIndex ON $testTable
             | (name, age)
             | WITH (
             |   incremental_refresh = true,
             |   checkpoint_location = '${checkpointDir.getAbsolutePath}'
             | )
             | """.stripMargin)

      // Refresh all present source data as of now
      sql(s"REFRESH INDEX $testIndex ON $testTable")
      flint.queryIndex(testFlintIndex).count() shouldBe 2

      // New data won't be refreshed until refresh statement triggered
      sql(s"""
             | INSERT INTO $testTable
             | PARTITION (year=2023, month=5)
             | VALUES ('Hello', 50, 'Vancouver')
             |""".stripMargin)
      flint.queryIndex(testFlintIndex).count() shouldBe 2

      // New data is refreshed incrementally
      sql(s"REFRESH INDEX $testIndex ON $testTable")
      flint.queryIndex(testFlintIndex).count() shouldBe 3
    }
  }

  test("create covering index on table without database name") {
    sql(s"CREATE INDEX $testIndex ON covering_sql_test (name)")

    flint.describeIndex(testFlintIndex) shouldBe defined
  }

  test("create covering index on table in other database") {
    sql("CREATE SCHEMA sample")
    sql("USE sample")

    // Create index without database name specified
    sql("CREATE TABLE test1 (name STRING) USING CSV")
    sql(s"CREATE INDEX $testIndex ON sample.test1 (name)")

    // Create index with database name specified
    sql("CREATE TABLE test2 (name STRING) USING CSV")
    sql(s"CREATE INDEX $testIndex ON sample.test2 (name)")

    try {
      flint.describeIndex(s"flint_spark_catalog_sample_test1_${testIndex}_index") shouldBe defined
      flint.describeIndex(s"flint_spark_catalog_sample_test2_${testIndex}_index") shouldBe defined
    } finally {
      sql("DROP DATABASE sample CASCADE")
    }
  }

  test("create covering index on table in other database than current") {
    sql("CREATE SCHEMA sample")
    sql("USE sample")

    // Specify database "default" in table name instead of current "sample" database
    sql(s"CREATE INDEX $testIndex ON $testTable (name)")

    try {
      flint.describeIndex(testFlintIndex) shouldBe defined
    } finally {
      sql("DROP DATABASE sample CASCADE")
    }
  }

  test("create covering index if not exists") {
    sql(s"""
           | CREATE INDEX IF NOT EXISTS $testIndex
           | ON $testTable (name, age)
           |""".stripMargin)
    flint.describeIndex(testFlintIndex) shouldBe defined

    // Expect error without IF NOT EXISTS, otherwise success
    assertThrows[IllegalStateException] {
      sql(s"""
             | CREATE INDEX $testIndex
             | ON $testTable (name, age)
             |""".stripMargin)
    }
    sql(s"""
           | CREATE INDEX IF NOT EXISTS $testIndex
           | ON $testTable (name, age)
           |""".stripMargin)
  }

  test("create skipping index with quoted index, table and column name") {
    sql(s"""
           | CREATE INDEX `$testIndex` ON `spark_catalog`.`default`.`covering_sql_test`
           | (`name`, `age`)
           | """.stripMargin)

    val index = flint.describeIndex(testFlintIndex)
    index shouldBe defined

    val metadata = index.get.metadata()
    metadata.name shouldBe testIndex
    metadata.source shouldBe testTable
    metadata.indexedColumns.map(_.asScala("columnName")) shouldBe Seq("name", "age")
  }

  test("rewrite applicable simple query with covering index") {
    awaitRefreshComplete(s"""
           | CREATE INDEX $testIndex ON $testTable
           | (name, age)
           | WITH (auto_refresh = true)
           | """.stripMargin)

    val query = s"SELECT name, age FROM $testTable"
    checkKeywordsExist(sql(s"EXPLAIN $query"), "FlintScan")
    checkAnswer(sql(query), Seq(Row("Hello", 30), Row("World", 25)))
  }

  test("rewrite applicable simple query with partial covering index") {
    awaitRefreshComplete(s"""
           | CREATE INDEX $testIndex ON $testTable
           | (name, age)
           | WHERE age > 25
           | WITH (auto_refresh = true)
           | """.stripMargin)

    val query = s"SELECT name, age FROM $testTable WHERE age >= 30"
    checkKeywordsExist(sql(s"EXPLAIN $query"), "FlintScan")
    checkAnswer(sql(query), Seq(Row("Hello", 30)))
  }

  test("rewrite applicable aggregate query with covering index") {
    awaitRefreshComplete(s"""
            | CREATE INDEX $testIndex ON $testTable
            | (name, age)
            | WITH (auto_refresh = true)
            | """.stripMargin)

    val query = s"""
                | SELECT age, COUNT(*) AS count
                | FROM $testTable
                | WHERE name = 'Hello'
                | GROUP BY age
                | ORDER BY count
                | """.stripMargin
    checkKeywordsExist(sql(s"EXPLAIN $query"), "FlintScan")
    checkAnswer(sql(query), Row(30, 1))
  }

  test("should not rewrite with covering index if disabled") {
    awaitRefreshComplete(s"""
           | CREATE INDEX $testIndex ON $testTable
           | (name, age)
           | WITH (auto_refresh = true)
           |""".stripMargin)

    spark.conf.set(OPTIMIZER_RULE_COVERING_INDEX_ENABLED.key, "false")
    try {
      checkKeywordsNotExist(sql(s"EXPLAIN SELECT name, age FROM $testTable"), "FlintScan")
    } finally {
      spark.conf.set(OPTIMIZER_RULE_COVERING_INDEX_ENABLED.key, "true")
    }
  }

  test("should not rewrite with partial covering index if not applicable") {
    awaitRefreshComplete(s"""
           | CREATE INDEX $testIndex ON $testTable
           | (name, age)
           | WHERE age > 25
           | WITH (auto_refresh = true)
           | """.stripMargin)

    val query = s"SELECT name, age FROM $testTable WHERE age > 20"
    checkKeywordsNotExist(sql(s"EXPLAIN $query"), "FlintScan")
    checkAnswer(sql(query), Seq(Row("Hello", 30), Row("World", 25)))
  }

  test("rewrite applicable query with covering index before skipping index") {
    try {
      sql(s"""
           | CREATE SKIPPING INDEX ON $testTable
           | (age MIN_MAX)
           | """.stripMargin)
      awaitRefreshComplete(s"""
           | CREATE INDEX $testIndex ON $testTable
           | (name, age)
           | WITH (auto_refresh = true)
           | """.stripMargin)

      val query = s"SELECT name FROM $testTable WHERE age = 30"
      checkKeywordsExist(sql(s"EXPLAIN $query"), "FlintScan")
      checkAnswer(sql(query), Row("Hello"))
    } finally {
      deleteTestIndex(getSkippingIndexName(testTable))
    }
  }

  test("show all covering index on the source table") {
    flint
      .coveringIndex()
      .name(testIndex)
      .onTable(testTable)
      .addIndexColumns("name", "age")
      .create()

    // Create another covering index
    flint
      .coveringIndex()
      .name("idx_address")
      .onTable(testTable)
      .addIndexColumns("address")
      .create()

    // Create a skipping index which is expected to be filtered
    flint
      .skippingIndex()
      .onTable(testTable)
      .addPartitions("year", "month")
      .create()

    val result = sql(s"SHOW INDEX ON $testTable")
    checkAnswer(result, Seq(Row(testIndex), Row("idx_address")))

    deleteTestIndex(getFlintIndexName("idx_address", testTable), getSkippingIndexName(testTable))
  }

  test("show covering index on source table with the same prefix") {
    flint
      .coveringIndex()
      .name(testIndex)
      .onTable(testTable)
      .addIndexColumns("name", "age")
      .create()

    val testTable2 = s"${testTable}_2"
    withTable(testTable2) {
      // Create another table with same prefix
      createPartitionedAddressTable(testTable2)
      flint
        .coveringIndex()
        .name(testIndex)
        .onTable(testTable2)
        .addIndexColumns("address")
        .create()

      // Expect no testTable2 present
      val result = sql(s"SHOW INDEX ON $testTable")
      checkAnswer(result, Seq(Row(testIndex)))

      deleteTestIndex(getFlintIndexName(testIndex, testTable2))
    }
  }

  test("describe covering index") {
    flint
      .coveringIndex()
      .name(testIndex)
      .onTable(testTable)
      .addIndexColumns("name", "age")
      .create()

    val result = sql(s"DESC INDEX $testIndex ON $testTable")
    checkAnswer(result, Seq(Row("name", "string", "indexed"), Row("age", "int", "indexed")))
  }

  test("update covering index") {
    flint
      .coveringIndex()
      .name(testIndex)
      .onTable(testTable)
      .addIndexColumns("name", "age")
      .create()

    flint.describeIndex(testFlintIndex) shouldBe defined
    flint.queryIndex(testFlintIndex).count() shouldBe 0

    awaitRefreshComplete(s"""
         | ALTER INDEX $testIndex ON $testTable
         | WITH (auto_refresh = true)
         | """.stripMargin)

    flint.queryIndex(testFlintIndex).count() shouldBe 2
  }

  test("drop and vacuum covering index") {
    flint
      .coveringIndex()
      .name(testIndex)
      .onTable(testTable)
      .addIndexColumns("name", "age")
      .create()

    sql(s"DROP INDEX $testIndex ON $testTable")
    sql(s"VACUUM INDEX $testIndex ON $testTable")
    flint.describeIndex(testFlintIndex) shouldBe empty
  }

  test("vacuum covering index with checkpoint") {
    withTempDir { checkpointDir =>
      flint
        .coveringIndex()
        .name(testIndex)
        .onTable(testTable)
        .addIndexColumns("name", "age")
        .options(
          FlintSparkIndexOptions(
            Map(
              "auto_refresh" -> "true",
              "checkpoint_location" -> checkpointDir.getAbsolutePath)),
          testFlintIndex)
        .create()
      flint.refreshIndex(testFlintIndex)

      val job = spark.streams.active.find(_.name == testFlintIndex)
      awaitStreamingComplete(job.get.id.toString)
      flint.deleteIndex(testFlintIndex)

      // Checkpoint folder should be removed after vacuum
      checkpointDir.exists() shouldBe true
      sql(s"VACUUM INDEX $testIndex ON $testTable")
      flint.describeIndex(testFlintIndex) shouldBe empty
      checkpointDir.exists() shouldBe false
    }
  }

  private def awaitRefreshComplete(query: String): Unit = {
    sql(query)

    // Wait for streaming job complete current micro batch
    val job = spark.streams.active.find(_.name == testFlintIndex)
    awaitStreamingComplete(job.get.id.toString)
  }
}
