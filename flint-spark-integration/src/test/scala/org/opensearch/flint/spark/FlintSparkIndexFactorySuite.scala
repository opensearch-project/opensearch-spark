/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import org.opensearch.flint.core.storage.FlintOpenSearchIndexMetadataService
import org.opensearch.flint.spark.mv.FlintSparkMaterializedView
import org.opensearch.flint.spark.mv.FlintSparkMaterializedView.MV_INDEX_TYPE
import org.scalatest.matchers.should.Matchers._

import org.apache.spark.FlintSuite

class FlintSparkIndexFactorySuite extends FlintSuite {

  /** Test table, MV name and query */
  val testTable = "spark_catalog.default.mv_build_test"
  val testMvName = "spark_catalog.default.mv"
  val testQuery = s"SELECT * FROM $testTable"

  test("create mv should generate source tables if missing in metadata") {
    val content =
      s""" {
        |    "_meta": {
        |      "kind": "$MV_INDEX_TYPE",
        |      "indexedColumns": [
        |        {
        |          "columnType": "int",
        |          "columnName": "age"
        |        }
        |      ],
        |      "name": "$testMvName",
        |      "source": "SELECT age FROM $testTable"
        |    },
        |    "properties": {
        |      "age": {
        |        "type": "integer"
        |      }
        |    }
        | }
        |""".stripMargin

    val metadata = FlintOpenSearchIndexMetadataService.deserialize(content)
    val index = FlintSparkIndexFactory.create(spark, metadata)
    index shouldBe defined
    index.get
      .asInstanceOf[FlintSparkMaterializedView]
      .sourceTables should contain theSameElementsAs Array(testTable)
  }
}
