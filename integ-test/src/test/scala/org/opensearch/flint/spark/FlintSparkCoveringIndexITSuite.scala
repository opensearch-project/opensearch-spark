/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import com.stephenn.scalatest.jsonassert.JsonMatchers.matchJson
import org.opensearch.flint.spark.FlintSpark.RefreshMode.{FULL, INCREMENTAL}
import org.scalatest.matchers.must.Matchers.defined
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import org.apache.spark.sql.Row

class FlintSparkCoveringIndexITSuite extends FlintSparkSuite {

  /** Test table and index name */
  private val testTable = "default.test_ci"
  private val testIndex = "ci"

  override def beforeAll(): Unit = {
    super.beforeAll()

    sql(s"""
           | CREATE TABLE $testTable
           | (
           |   name STRING,
           |   age INT,
           |   address STRING
           | )
           | USING CSV
           | OPTIONS (
           |  header 'false',
           |  delimiter '\t'
           | )
           | PARTITIONED BY (
           |    year INT,
           |    month INT
           | )
           |""".stripMargin)

    sql(s"""
           | INSERT INTO $testTable
           | PARTITION (year=2023, month=4)
           | VALUES ('Hello', 30, 'Seattle')
           | """.stripMargin)

    sql(s"""
           | INSERT INTO $testTable
           | PARTITION (year=2023, month=5)
           | VALUES ('World', 25, 'Portland')
           | """.stripMargin)
  }

  override def afterEach(): Unit = {
    super.afterEach()

    // Delete all test indices
    flint.deleteIndex(testIndex)
  }

  test("create covering index with metadata successfully") {
    flint
      .coveringIndex()
      .indexName(testIndex)
      .onTable(testTable)
      .addIndexColumn("name", "age")
      .create()

    val index = flint.describeIndex(testIndex)
    index shouldBe defined
    index.get.metadata().getContent should matchJson(s"""{
         |   "_meta": {
         |     "name": "ci",
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
         |     "source": "default.test_ci"
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
      .indexName(testIndex)
      .onTable(testTable)
      .addIndexColumn("name", "age")
      .create()

    flint.refreshIndex(testIndex, FULL)

    val indexData = flint.queryIndex(testIndex)
    checkAnswer(indexData, Seq(Row("Hello", 30), Row("World", 25)))
  }

  test("incremental refresh covering index successfully") {
    flint
      .coveringIndex()
      .indexName(testIndex)
      .onTable(testTable)
      .addIndexColumn("name", "age")
      .create()

    val jobId = flint.refreshIndex(testIndex, INCREMENTAL)
    jobId shouldBe defined

    val job = spark.streams.get(jobId.get)
    failAfter(streamingTimeout) {
      job.processAllAvailable()
    }

    val indexData = flint.queryIndex(testIndex)
    checkAnswer(indexData, Seq(Row("Hello", 30), Row("World", 25)))
  }
}
