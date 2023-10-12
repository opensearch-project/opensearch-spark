/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import scala.Option.empty
import scala.collection.JavaConverters.mapAsJavaMapConverter

import org.json4s.{Formats, NoTypeHints}
import org.json4s.native.JsonMethods.parse
import org.json4s.native.Serialization
import org.opensearch.flint.core.FlintOptions
import org.opensearch.flint.core.storage.FlintOpenSearchClient
import org.opensearch.flint.spark.covering.FlintSparkCoveringIndex.getFlintIndexName
import org.opensearch.flint.spark.skipping.FlintSparkSkippingIndex.getSkippingIndexName
import org.scalatest.matchers.must.Matchers.defined
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import org.apache.spark.sql.Row

class FlintSparkCoveringIndexSqlITSuite extends FlintSparkSuite {

  /** Test table and index name */
  private val testTable = "spark_catalog.default.covering_sql_test"
  private val testIndex = "name_and_age"
  private val testFlintIndex = getFlintIndexName(testIndex, testTable)

  override def beforeAll(): Unit = {
    super.beforeAll()

    createPartitionedTable(testTable)
  }

  override def afterEach(): Unit = {
    super.afterEach()

    // Delete all test indices
    flint.deleteIndex(testFlintIndex)
  }

  test("create covering index with auto refresh") {
    sql(s"""
         | CREATE INDEX $testIndex ON $testTable
         | (name, age)
         | WITH (auto_refresh = true)
         |""".stripMargin)

    // Wait for streaming job complete current micro batch
    val job = spark.streams.active.find(_.name == testFlintIndex)
    job shouldBe defined
    failAfter(streamingTimeout) {
      job.get.processAllAvailable()
    }

    val indexData = flint.queryIndex(testFlintIndex)
    indexData.count() shouldBe 2
  }

  test("create skipping index with streaming job options") {
    withTempDir { checkpointDir =>
      sql(s"""
             | CREATE INDEX $testIndex ON $testTable ( name )
             | WITH (
             |   auto_refresh = true,
             |   refresh_interval = '5 Seconds',
             |   checkpoint_location = '${checkpointDir.getAbsolutePath}'
             | )
             | """.stripMargin)

      val index = flint.describeIndex(testFlintIndex)
      index shouldBe defined
      index.get.options.autoRefresh() shouldBe true
      index.get.options.refreshInterval() shouldBe Some("5 Seconds")
      index.get.options.checkpointLocation() shouldBe Some(checkpointDir.getAbsolutePath)
    }
  }

  test("create skipping index with index settings") {
    sql(s"""
           | CREATE INDEX $testIndex ON $testTable ( name )
           | WITH (
           |   index_settings = '{"number_of_shards": 2, "number_of_replicas": 3}'
           | )
           |""".stripMargin)

    // Check if the index setting option is set to OS index setting
    val flintClient = new FlintOpenSearchClient(new FlintOptions(openSearchOptions.asJava))

    implicit val formats: Formats = Serialization.formats(NoTypeHints)
    val settings = parse(flintClient.getIndexMetadata(testFlintIndex).indexSettings.get)
    (settings \ "index.number_of_shards").extract[String] shouldBe "2"
    (settings \ "index.number_of_replicas").extract[String] shouldBe "3"
  }

  test("create covering index with manual refresh") {
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

    flint.deleteIndex(getFlintIndexName("idx_address", testTable))
    flint.deleteIndex(getSkippingIndexName(testTable))
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

  test("drop covering index") {
    flint
      .coveringIndex()
      .name(testIndex)
      .onTable(testTable)
      .addIndexColumns("name", "age")
      .create()

    sql(s"DROP INDEX $testIndex ON $testTable")

    flint.describeIndex(testFlintIndex) shouldBe empty
  }
}
