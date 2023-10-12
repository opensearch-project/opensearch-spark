/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.mv

import scala.collection.JavaConverters.mapAsJavaMapConverter

import org.opensearch.flint.spark.FlintSparkIndexOptions
import org.opensearch.flint.spark.mv.FlintSparkMaterializedView.MV_INDEX_TYPE
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import org.apache.spark.FlintSuite

class FlintSparkMaterializedViewSuite extends FlintSuite {

  val testMvName = "spark_catalog.default.mv"
  val testQuery = "SELECT 1"

  test("get MV name") {
    val mv = FlintSparkMaterializedView(testMvName, testQuery, Map.empty)
    mv.name() shouldBe "flint_spark_catalog_default_mv"
  }

  test("should fail if not full MV name") {
    val mv = FlintSparkMaterializedView("mv", "SELECT 1", Map.empty)
    assertThrows[IllegalArgumentException] {
      mv.name()
    }
  }

  test("get MV metadata") {
    val mv = FlintSparkMaterializedView(testMvName, testQuery, Map("test_col" -> "integer"))

    val metadata = mv.metadata()
    metadata.name shouldBe mv.mvName
    metadata.kind shouldBe MV_INDEX_TYPE
    metadata.source shouldBe "SELECT 1"
    metadata.indexedColumns shouldBe Array(
      Map("columnName" -> "test_col", "columnType" -> "integer").asJava)
    metadata.schema shouldBe Map("test_col" -> Map("type" -> "integer").asJava).asJava
  }

  test("get MV metadata with index options") {
    val indexSettings = """{"number_of_shards": 2}"""
    val indexOptions =
      FlintSparkIndexOptions(Map("auto_refresh" -> "true", "index_settings" -> indexSettings))
    val mv = FlintSparkMaterializedView(
      testMvName,
      testQuery,
      Map("test_col" -> "integer"),
      indexOptions)

    mv.metadata().options shouldBe Map(
      "auto_refresh" -> "true",
      "index_settings" -> indexSettings).asJava
    mv.metadata().indexSettings shouldBe Some(indexSettings)
  }
}
