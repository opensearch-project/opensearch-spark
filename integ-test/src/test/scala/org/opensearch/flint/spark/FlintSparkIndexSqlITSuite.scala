/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import org.opensearch.client.RequestOptions
import org.opensearch.client.indices.CreateIndexRequest
import org.opensearch.common.xcontent.XContentType
import org.opensearch.flint.spark.FlintSparkIndexOptions.OptionName.AUTO_REFRESH
import org.opensearch.flint.spark.covering.FlintSparkCoveringIndex
import org.opensearch.flint.spark.mv.FlintSparkMaterializedView
import org.opensearch.flint.spark.skipping.FlintSparkSkippingIndex

import org.apache.spark.sql.Row

class FlintSparkIndexSqlITSuite extends FlintSparkSuite {

  private val testTableName = "index_test"
  private val testTableQualifiedName = s"spark_catalog.default.$testTableName"
  private val testCoveringIndex = "name_age"
  private val testMvIndexShortName = "mv1"
  private val testMvQuery = s"SELECT name, age FROM $testTableQualifiedName"

  private val testSkippingFlintIndex =
    FlintSparkSkippingIndex.getSkippingIndexName(testTableQualifiedName)
  private val testCoveringFlintIndex =
    FlintSparkCoveringIndex.getFlintIndexName(testCoveringIndex, testTableQualifiedName)
  private val testMvIndex = s"spark_catalog.default.$testMvIndexShortName"
  private val testMvFlintIndex = FlintSparkMaterializedView.getFlintIndexName(testMvIndex)

  override def beforeAll(): Unit = {
    super.beforeAll()
    createTimeSeriesTable(testTableQualifiedName)
  }

  test("show all flint indexes in catalog and database") {
    // Show in catalog
    flint
      .materializedView()
      .name(testMvIndex)
      .query(testMvQuery)
      .create()

    flint
      .coveringIndex()
      .name(testCoveringIndex)
      .onTable(testTableQualifiedName)
      .addIndexColumns("name", "age")
      .create()

    flint
      .skippingIndex()
      .onTable(testTableQualifiedName)
      .addValueSet("name")
      .create()

    checkAnswer(
      sql(s"SHOW FLINT INDEX IN spark_catalog"),
      Seq(
        Row(testMvFlintIndex, "mv", "default", null, testMvIndexShortName, false, "active"),
        Row(
          testCoveringFlintIndex,
          "covering",
          "default",
          testTableName,
          testCoveringIndex,
          false,
          "active"),
        Row(testSkippingFlintIndex, "skipping", "default", testTableName, null, false, "active")))

    // Create index in other database
    flint
      .materializedView()
      .name("spark_catalog.other.mv2")
      .query(testMvQuery)
      .create()

    // Show in catalog.database shouldn't show index in other database
    checkAnswer(
      sql(s"SHOW FLINT INDEX IN spark_catalog.default"),
      Seq(
        Row(testMvFlintIndex, "mv", "default", null, testMvIndexShortName, false, "active"),
        Row(
          testCoveringFlintIndex,
          "covering",
          "default",
          testTableName,
          testCoveringIndex,
          false,
          "active"),
        Row(testSkippingFlintIndex, "skipping", "default", testTableName, null, false, "active")))

    deleteTestIndex(
      testMvFlintIndex,
      testCoveringFlintIndex,
      testSkippingFlintIndex,
      FlintSparkMaterializedView.getFlintIndexName("spark_catalog.other.mv2"))
  }

  test("should return empty when show flint index in empty database") {
    checkAnswer(sql(s"SHOW FLINT INDEX IN spark_catalog.default"), Seq.empty)
  }

  test("show flint index with auto refresh") {
    flint
      .coveringIndex()
      .name(testCoveringIndex)
      .onTable(testTableQualifiedName)
      .addIndexColumns("name", "age")
      .options(FlintSparkIndexOptions(Map(AUTO_REFRESH.toString -> "true")))
      .create()
    flint.refreshIndex(testCoveringFlintIndex)

    checkAnswer(
      sql(s"SHOW FLINT INDEX IN spark_catalog"),
      Seq(
        Row(
          testCoveringFlintIndex,
          "covering",
          "default",
          testTableName,
          testCoveringIndex,
          true,
          "refreshing")))
    deleteTestIndex(testCoveringFlintIndex)
  }

  test("show flint index in database with the same prefix") {
    flint.materializedView().name("spark_catalog.default.mv1").query(testMvQuery).create()
    flint.materializedView().name("spark_catalog.default_test.mv2").query(testMvQuery).create()
    checkAnswer(
      sql(s"SHOW FLINT INDEX IN spark_catalog.default").select("index_name"),
      Seq(Row("mv1")))

    deleteTestIndex(
      FlintSparkMaterializedView.getFlintIndexName("spark_catalog.default.mv1"),
      FlintSparkMaterializedView.getFlintIndexName("spark_catalog.default_test.mv2"))
  }

  test("should ignore non-Flint index") {
    try {
      sql(s"CREATE SKIPPING INDEX ON $testTableQualifiedName (name VALUE_SET)")

      // Create a non-Flint index which has "flint_" prefix in coincidence
      openSearchClient
        .indices()
        .create(
          new CreateIndexRequest("flint_spark_catalog_invalid_index1"),
          RequestOptions.DEFAULT)

      // Create a non-Flint index which has "flint_" prefix and _meta mapping in coincidence
      openSearchClient
        .indices()
        .create(
          new CreateIndexRequest("flint_spark_catalog_invalid_index2")
            .mapping(
              """{
                |  "_meta": {
                |    "custom": "test"
                |  }
                |}
                |""".stripMargin,
              XContentType.JSON),
          RequestOptions.DEFAULT)

      // Show statement should ignore such index without problem
      checkAnswer(
        sql(s"SHOW FLINT INDEX IN spark_catalog"),
        Row(testSkippingFlintIndex, "skipping", "default", testTableName, null, false, "active"))
    } finally {
      deleteTestIndex(testSkippingFlintIndex)
    }
  }
}
