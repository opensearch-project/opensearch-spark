/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.opensearch.table

import org.apache.spark.sql.Row

class OpenSearchTableITSuite extends OpenSearchCatalogSuite {

  def multipleShardsIndex(indexName: String): Unit = {
    val twoShards = """{
                           |  "number_of_shards": "2",
                           |  "number_of_replicas": "0"
                           |}""".stripMargin

    val mappings = """{
                     |  "properties": {
                     |    "accountId": {
                     |      "type": "keyword"
                     |    },
                     |    "eventName": {
                     |      "type": "keyword"
                     |    },
                     |    "eventSource": {
                     |      "type": "keyword"
                     |    }
                     |  }
                     |}""".stripMargin
    val docs = Seq("""{
                     |  "accountId": "123",
                     |  "eventName": "event",
                     |  "eventSource": "source"
                     |}""".stripMargin)
    index(indexName, twoShards, mappings, docs)
  }

  test("Partition works correctly when indices include multiple shards") {
    val indexName1 = "t0001"
    withIndexName(indexName1) {
      multipleShardsIndex(indexName1)
      val df = spark.sql(s"""
        SELECT accountId, eventName, eventSource
        FROM ${catalogName}.default.`t0001`""")

      assert(df.rdd.getNumPartitions == 2)
    }
  }

  test("Partition works correctly when query wildcard index") {
    val indexName1 = "t0001"
    val indexName2 = "t0002"
    withIndexName(indexName1) {
      withIndexName(indexName2) {
        simpleIndex(indexName1)
        simpleIndex(indexName2)
        val df = spark.sql(s"""
        SELECT accountId, eventName, eventSource
        FROM ${catalogName}.default.`t0001,t0002`""")

        assert(df.rdd.getNumPartitions == 2)
      }
    }
  }

  test("Query index with alias data type") {
    val index1 = "t0001"
    withIndexName(index1) {
      indexWithAlias(index1)
      // select original field and alias field
      var df = spark.sql(s"""SELECT id, alias FROM ${catalogName}.default.$index1""")
      checkAnswer(df, Seq(Row(1, 1), Row(2, 2)))

      // filter on alias field
      df = spark.sql(s"""SELECT id, alias FROM ${catalogName}.default.$index1 WHERE alias=1""")
      checkAnswer(df, Row(1, 1))

      // filter on original field
      df = spark.sql(s"""SELECT id, alias FROM ${catalogName}.default.$index1 WHERE id=1""")
      checkAnswer(df, Row(1, 1))
    }
  }
}
