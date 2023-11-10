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
import org.opensearch.flint.core.storage.FlintOpenSearchClient
import org.opensearch.flint.spark.skipping.FlintSparkSkippingIndex.getSkippingIndexName
import org.scalatest.matchers.must.Matchers.{defined, include}
import org.scalatest.matchers.should.Matchers.{convertToAnyShouldWrapper, the}

import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.SimpleMode
import org.apache.spark.sql.flint.FlintDataSourceV2.FLINT_DATASOURCE
import org.apache.spark.sql.flint.config.FlintSparkConf.CHECKPOINT_MANDATORY

class FlintSparkSkippingIndexSqlITSuite extends FlintSparkSuite {

  /** Test table and index name */
  private val testTable = "spark_catalog.default.skipping_sql_test"
  private val testIndex = getSkippingIndexName(testTable)

  override def beforeAll(): Unit = {
    super.beforeAll()

    createPartitionedTable(testTable)
  }

  protected override def afterEach(): Unit = {
    super.afterEach()

    flint.deleteIndex(testIndex)
  }

  test("create skipping index with auto refresh") {
    sql(s"""
           | CREATE SKIPPING INDEX ON $testTable
           | (
           |   year PARTITION,
           |   name VALUE_SET,
           |   age MIN_MAX
           | )
           | WITH (auto_refresh = true)
           | """.stripMargin)

    val indexData = awaitStreamingDataComplete(testIndex)
    flint.describeIndex(testIndex) shouldBe defined
    indexData.count() shouldBe 2
  }

  test("create skipping index with auto refresh and filtering condition") {
    val testTimeSeriesTable = "spark_catalog.default.partial_skipping_sql_test"
    val testFlintTimeSeriesTable = getSkippingIndexName(testTimeSeriesTable)

    withTable(testTimeSeriesTable) {
      createTimeSeriesTable(testTimeSeriesTable)
      sql(s""" CREATE SKIPPING INDEX ON $testTimeSeriesTable
             | ( address VALUE_SET )
             |  WHERE time >= '2023-10-01 01:00:00'
             |  WITH (auto_refresh = true)""".stripMargin)
      flint.describeIndex(testFlintTimeSeriesTable) shouldBe defined

      // Only 2 rows indexed
      val indexData = awaitStreamingDataComplete(testFlintTimeSeriesTable)
      indexData.count() shouldBe 2

      // Query without filter condition
      sql(s"SELECT * FROM $testTimeSeriesTable").count shouldBe 5

      // File indexed should be included
      var query = sql(s""" SELECT name FROM $testTimeSeriesTable
                         | WHERE time > '2023-10-01 00:05:00'
                         |   AND address = 'Portland' """.stripMargin)
      query.queryExecution.explainString(SimpleMode) should include("FlintSparkSkippingFileIndex")
      checkAnswer(query, Seq(Row("C"), Row("D")))

      // File not indexed should be included too
      query = sql(s""" SELECT name FROM $testTimeSeriesTable
                     | WHERE time > '2023-10-01 00:05:00'
                     |   AND address = 'Seattle' """.stripMargin)
      query.queryExecution.explainString(SimpleMode) should include("FlintSparkSkippingFileIndex")
      checkAnswer(query, Seq(Row("B")))

      flint.deleteIndex(testFlintTimeSeriesTable)
    }
  }

  test("create skipping index with manual refresh and filtering condition") {
    val testTimeSeriesTable = "spark_catalog.default.partial_skipping_sql_test"
    val testFlintTimeSeriesTable = getSkippingIndexName(testTimeSeriesTable)

    withTable(testTimeSeriesTable) {
      createTimeSeriesTable(testTimeSeriesTable)
      sql(s""" CREATE SKIPPING INDEX ON $testTimeSeriesTable
             | ( address VALUE_SET )
             | WHERE time >= '2023-10-01 01:00:00' AND age = 15
             | """.stripMargin)
      sql(s"REFRESH SKIPPING INDEX ON $testTimeSeriesTable")

      // Only 2 rows indexed
      flint.describeIndex(testFlintTimeSeriesTable) shouldBe defined
      val indexData = flint.queryIndex(testFlintTimeSeriesTable)
      indexData.count() shouldBe 1

      // File not indexed should be included too
      sql(s"SELECT * FROM $testTimeSeriesTable").count shouldBe 5
      var query = sql(s""" SELECT name FROM $testTimeSeriesTable
                         | WHERE time > '2023-10-01 00:05:00'
                         |   AND address = 'Portland' """.stripMargin)
      query.queryExecution.explainString(SimpleMode) should include("FlintSparkSkippingFileIndex")
      checkAnswer(query, Seq(Row("C"), Row("D")))

      // Generate new data
      sql(s""" INSERT INTO $testTimeSeriesTable VALUES
             | (TIMESTAMP '2023-10-01 04:00:00', 'F', 30, 'Vancouver')""".stripMargin)

      // Latest file should be included too without refresh
      sql(s"SELECT * FROM $testTimeSeriesTable").count shouldBe 6
      query = sql(s""" SELECT name FROM $testTimeSeriesTable
                     | WHERE time > '2023-10-01 00:05:00'
                     |   AND address = 'Vancouver' """.stripMargin)
      query.queryExecution.explainString(SimpleMode) should include("FlintSparkSkippingFileIndex")
      checkAnswer(query, Seq(Row("E"), Row("F")))

      flint.deleteIndex(testFlintTimeSeriesTable)
    }
  }

  test("create skipping index with streaming job options") {
    withTempDir { checkpointDir =>
      sql(s"""
             | CREATE SKIPPING INDEX ON $testTable
             | ( year PARTITION )
             | WITH (
             |   auto_refresh = true,
             |   refresh_interval = '5 Seconds',
             |   checkpoint_location = '${checkpointDir.getAbsolutePath}',
             |   extra_options = '{"$testTable": {"maxFilesPerTrigger": "1"}}'
             | )
             | """.stripMargin)

      val index = flint.describeIndex(testIndex)
      index shouldBe defined
      index.get.options.autoRefresh() shouldBe true
      index.get.options.refreshInterval() shouldBe Some("5 Seconds")
      index.get.options.checkpointLocation() shouldBe Some(checkpointDir.getAbsolutePath)
    }
  }

  test("create skipping index with index settings") {
    sql(s"""
           | CREATE SKIPPING INDEX ON $testTable
           | ( year PARTITION )
           | WITH (
           |   index_settings = '{"number_of_shards": 3, "number_of_replicas": 2}'
           | )
           |""".stripMargin)

    // Check if the index setting option is set to OS index setting
    val flintClient = new FlintOpenSearchClient(new FlintOptions(openSearchOptions.asJava))

    implicit val formats: Formats = Serialization.formats(NoTypeHints)
    val settings = parse(flintClient.getIndexMetadata(testIndex).indexSettings.get)
    (settings \ "index.number_of_shards").extract[String] shouldBe "3"
    (settings \ "index.number_of_replicas").extract[String] shouldBe "2"
  }

  test("create skipping index with invalid option") {
    the[IllegalArgumentException] thrownBy
      sql(s"""
             | CREATE SKIPPING INDEX ON $testTable
             | ( year PARTITION )
             | WITH (autoRefresh = true)
             | """.stripMargin)
  }

  test("create skipping index with auto refresh should fail if mandatory checkpoint enabled") {
    setFlintSparkConf(CHECKPOINT_MANDATORY, "true")
    try {
      the[IllegalStateException] thrownBy {
        sql(s"""
               | CREATE SKIPPING INDEX ON $testTable
               | ( year PARTITION )
               | WITH (auto_refresh = true)
               | """.stripMargin)
      }
    } finally {
      setFlintSparkConf(CHECKPOINT_MANDATORY, "false")
    }
  }

  test("create skipping index with manual refresh") {
    sql(s"""
         | CREATE SKIPPING INDEX ON $testTable
         | (
         |   year PARTITION,
         |   name VALUE_SET,
         |   age MIN_MAX
         | )
         | """.stripMargin)

    val indexData = spark.read.format(FLINT_DATASOURCE).load(testIndex)

    flint.describeIndex(testIndex) shouldBe defined
    indexData.count() shouldBe 0

    sql(s"REFRESH SKIPPING INDEX ON $testTable")
    indexData.count() shouldBe 2
  }

  test("create skipping index if not exists") {
    sql(s"""
           | CREATE SKIPPING INDEX
           | IF NOT EXISTS
           | ON $testTable ( year PARTITION )
           | """.stripMargin)
    flint.describeIndex(testIndex) shouldBe defined

    // Expect error without IF NOT EXISTS, otherwise success
    assertThrows[IllegalStateException] {
      sql(s"""
             | CREATE SKIPPING INDEX
             | ON $testTable ( year PARTITION )
             | """.stripMargin)
    }
    sql(s"""
           | CREATE SKIPPING INDEX
           | IF NOT EXISTS
           | ON $testTable ( year PARTITION )
           | """.stripMargin)
  }

  test("create skipping index with quoted table and column name") {
    sql(s"""
           | CREATE SKIPPING INDEX ON `spark_catalog`.`default`.`skipping_sql_test`
           | (
           |   `year` PARTITION,
           |   `name` VALUE_SET,
           |   `age` MIN_MAX
           | )
           | """.stripMargin)

    val index = flint.describeIndex(testIndex)
    index shouldBe defined

    val metadata = index.get.metadata()
    metadata.source shouldBe testTable
    metadata.indexedColumns.map(_.asScala("columnName")) shouldBe Seq("year", "name", "age")
  }

  test("describe skipping index") {
    flint
      .skippingIndex()
      .onTable(testTable)
      .addPartitions("year")
      .addValueSet("name")
      .addMinMax("age")
      .create()

    val result = sql(s"DESC SKIPPING INDEX ON $testTable")

    checkAnswer(
      result,
      Seq(
        Row("year", "int", "PARTITION"),
        Row("name", "string", "VALUE_SET"),
        Row("age", "int", "MIN_MAX")))
  }

  test("create skipping index on table without database name") {
    sql("CREATE SKIPPING INDEX ON skipping_sql_test ( year PARTITION )")

    flint.describeIndex(testIndex) shouldBe defined
  }

  test("create skipping index on table in other database") {
    sql("CREATE SCHEMA sample")
    sql("USE sample")

    // Create index without database name specified
    sql("CREATE TABLE test1 (name STRING) USING CSV")
    sql("CREATE SKIPPING INDEX ON test1 (name VALUE_SET)")

    // Create index with database name specified
    sql("CREATE TABLE test2 (name STRING) USING CSV")
    sql("CREATE SKIPPING INDEX ON sample.test2 (name VALUE_SET)")

    try {
      flint.describeIndex("flint_spark_catalog_sample_test1_skipping_index") shouldBe defined
      flint.describeIndex("flint_spark_catalog_sample_test2_skipping_index") shouldBe defined
    } finally {
      sql("DROP DATABASE sample CASCADE")
    }
  }

  test("create skipping index on table in other database than current") {
    sql("CREATE SCHEMA sample")
    sql("USE sample")

    // Specify database "default" in table name instead of current "sample" database
    sql(s"CREATE SKIPPING INDEX ON $testTable (name VALUE_SET)")

    try {
      flint.describeIndex(testIndex) shouldBe defined
    } finally {
      sql("DROP DATABASE sample CASCADE")
    }
  }

  test("should return empty if no skipping index to describe") {
    val result = sql(s"DESC SKIPPING INDEX ON $testTable")

    checkAnswer(result, Seq.empty)
  }

  test("drop skipping index") {
    flint
      .skippingIndex()
      .onTable(testTable)
      .addPartitions("year")
      .create()

    sql(s"DROP SKIPPING INDEX ON $testTable")

    flint.describeIndex(testIndex) shouldBe empty
  }
}
