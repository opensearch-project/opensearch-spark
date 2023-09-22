/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import com.stephenn.scalatest.jsonassert.JsonMatchers.matchJson
import org.opensearch.flint.spark.mv.FlintSparkMaterializedView.getFlintIndexName
import org.scalatest.matchers.must.Matchers.defined
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class FlintSparkMaterializedViewITSuite extends FlintSparkSuite {

  private val testTable = "default.mv_test"
  private val testMv = "default.mv"
  private val testFlintIndex = getFlintIndexName(testMv)

  override def beforeAll(): Unit = {
    super.beforeAll()

    createPartitionedTable(testTable)
  }

  override def afterEach(): Unit = {
    super.afterEach()

    // Delete all test indices
    flint.deleteIndex(testFlintIndex)
  }

  test("create mv with metadata successfully") {
    flint
      .materializedView()
      .name(testMv)
      .query(s"""
             | SELECT
             |   window.start AS startTime,
             |   COUNT(*) AS count
             | FROM $testTable
             | GROUP BY TUMBLE(time, '1 Hour')
             |""".stripMargin)
      .create()

    val index = flint.describeIndex(testFlintIndex)
    index shouldBe defined

    val expectedSource =
      "\n SELECT\n   window.start AS startTime,\n   COUNT(*) AS count\n" +
        " FROM default.mv_test\n GROUP BY TUMBLE(time, '1 Hour')\n"
    index.get.metadata().getContent should matchJson(s"""
         | {
         |  "_meta": {
         |    "name": "default.mv",
         |    "kind": "mv",
         |    "indexedColumns": [
         |    {
         |      "columnName": "startTime",
         |      "columnType": "timestamp"
         |    },{
         |      "columnName": "count",
         |      "columnType": "long"
         |    }],
         |    "source": "$expectedSource",
         |    "options": {}
         |  },
         |  "properties": {
         |    "startTime": {
         |      "type": "date",
         |      "format": "strict_date_optional_time_nanos"
         |    },
         |    "count": {
         |      "type": "long"
         |    }
         |  }
         | }
         |""".stripMargin)
  }

  /*
  test("create mv with metadata successfully") {
    withTempDir { checkpointDir =>
      flint
        .materializedView()
        .name(testMv)
        .query(s"""
             | SELECT time, COUNT(*) AS count
             | FROM $testTable
             | GROUP BY TUMBLE(time, '1 Hour')
             |""".stripMargin)
        .options(
          FlintSparkIndexOptions(Map("checkpoint_location" -> checkpointDir.getAbsolutePath)))
        .create()
    }
  }
   */
}
