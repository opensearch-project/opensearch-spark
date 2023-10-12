/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import com.stephenn.scalatest.jsonassert.JsonMatchers.matchJson
import org.opensearch.flint.core.FlintVersion.current
import org.opensearch.flint.spark.mv.FlintSparkMaterializedView.getFlintIndexName
import org.scalatest.matchers.must.Matchers.defined
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class FlintSparkMaterializedViewITSuite extends FlintSparkSuite {

  private val testTable = "spark_catalog.default.ci_test"
  private val testMvName = "spark_catalog.default.mv_test"
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

    // Delete all test indices
    flint.deleteIndex(testFlintIndex)
  }

  test("create materialized view") {
    flint
      .materializedView()
      .name(testMvName)
      .query(testQuery)
      .create()

    val index = flint.describeIndex(testFlintIndex)
    index shouldBe defined
    index.get.metadata().getContent should matchJson(s"""
         | {
         |  "_meta": {
         |    "version": "${current()}",
         |    "name": "spark_catalog.default.mv_test",
         |    "kind": "mv",
         |    "source": "$testQuery",
         |    "indexedColumns": [
         |    {
         |      "columnName": "startTime",
         |      "columnType": "timestamp"
         |    },{
         |      "columnName": "count",
         |      "columnType": "long"
         |    }],
         |    "options": {},
         |    "properties": {}
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
}
