/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.opensearch.table

import org.opensearch.flint.spark.ppl.FlintPPLSuite

import org.apache.spark.sql.Row

/**
 * Test queries on OpenSearch Table
 */
class OpenSearchTableQueryITSuite extends OpenSearchCatalogSuite with FlintPPLSuite {
  test("SQL Join two indices") {
    val indexName1 = "t0001"
    val indexName2 = "t0002"
    withIndexName(indexName1) {
      withIndexName(indexName2) {
        simpleIndex(indexName1)
        simpleIndex(indexName2)
        val df = spark.sql(s"""
        SELECT t1.accountId, t2.eventName, t2.eventSource
        FROM ${catalogName}.default.$indexName1 as t1 JOIN ${catalogName}.default.$indexName2 as t2 ON
        t1.accountId == t2.accountId""")

        checkAnswer(df, Row("123", "event", "source"))
      }
    }
  }

  test("PPL Lookup") {
    val indexName1 = "t0001"
    val indexName2 = "t0002"
    val factTbl = s"${catalogName}.default.$indexName1"
    val lookupTbl = s"${catalogName}.default.$indexName2"
    withIndexName(indexName1) {
      withIndexName(indexName2) {
        simpleIndex(indexName1)
        simpleIndex(indexName2)

        val df = spark.sql(
          s"source = $factTbl | stats count() by accountId " +
            s"| LOOKUP $lookupTbl accountId REPLACE eventSource")
        checkAnswer(df, Row(1, "123", "source"))
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
