/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import java.util.Base64

import com.stephenn.scalatest.jsonassert.JsonMatchers.matchJson
import org.opensearch.flint.core.FlintVersion.current
import org.opensearch.flint.spark.covering.FlintSparkCoveringIndex.getFlintIndexName
import org.scalatest.matchers.must.Matchers.defined
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import org.apache.spark.sql.Row

class FlintSparkCoveringIndexITSuite extends FlintSparkSuite {

  /** Test table and index name */
  private val testTable = "spark_catalog.default.ci_test"
  private val testIndex = "name_and_age"
  private val testFlintIndex = getFlintIndexName(testIndex, testTable)
  private val testLatestId = Base64.getEncoder.encodeToString(testFlintIndex.getBytes)

  override def beforeAll(): Unit = {
    super.beforeAll()

    createPartitionedTable(testTable)
  }

  override def afterEach(): Unit = {
    super.afterEach()

    // Delete all test indices
    deleteTestIndex(testFlintIndex)
  }

  test("create covering index with metadata successfully") {
    flint
      .coveringIndex()
      .name(testIndex)
      .onTable(testTable)
      .addIndexColumns("name", "age")
      .filterBy("age > 30")
      .create()

    val index = flint.describeIndex(testFlintIndex)
    index shouldBe defined
    index.get.metadata().getContent should matchJson(s"""{
         |   "_meta": {
         |     "version": "${current()}",
         |     "name": "name_and_age",
         |     "kind": "covering",
         |     "indexedColumns": [
         |     {
         |        "columnName": "name",
         |        "columnType": "string"
         |     },
         |     {
         |        "columnName": "age",
         |        "columnType": "int"
         |     }],
         |     "source": "spark_catalog.default.ci_test",
         |     "options": {
         |       "auto_refresh": "false",
         |       "incremental_refresh": "false"
         |     },
         |     "latestId": "$testLatestId",
         |     "properties": {
         |       "filterCondition": "age > 30"
         |     }
         |   },
         |   "properties": {
         |     "name": {
         |       "type": "keyword"
         |     },
         |     "age": {
         |       "type": "integer"
         |     }
         |   }
         | }
         |""".stripMargin)
  }

  test("full refresh covering index successfully") {
    flint
      .coveringIndex()
      .name(testIndex)
      .onTable(testTable)
      .addIndexColumns("name", "age")
      .create()

    flint.refreshIndex(testFlintIndex)

    val indexData = flint.queryIndex(testFlintIndex)
    checkAnswer(indexData, Seq(Row("Hello", 30), Row("World", 25)))
  }

  test("incremental refresh covering index successfully") {
    flint
      .coveringIndex()
      .name(testIndex)
      .onTable(testTable)
      .addIndexColumns("name", "age")
      .options(FlintSparkIndexOptions(Map("auto_refresh" -> "true")))
      .create()

    val jobId = flint.refreshIndex(testFlintIndex)
    jobId shouldBe defined

    val job = spark.streams.get(jobId.get)
    failAfter(streamingTimeout) {
      job.processAllAvailable()
    }

    val indexData = flint.queryIndex(testFlintIndex)
    checkAnswer(indexData, Seq(Row("Hello", 30), Row("World", 25)))
  }

  test("can have multiple covering indexes on a table") {
    flint
      .coveringIndex()
      .name(testIndex)
      .onTable(testTable)
      .addIndexColumns("name", "age")
      .create()

    val newIndex = testIndex + "_address"
    flint
      .coveringIndex()
      .name(newIndex)
      .onTable(testTable)
      .addIndexColumns("address")
      .create()
    deleteTestIndex(getFlintIndexName(newIndex, testTable))
  }
}
