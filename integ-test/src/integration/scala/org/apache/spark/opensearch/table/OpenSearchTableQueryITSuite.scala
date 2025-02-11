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

  test("Query index with ip data type") {
    val index1 = "t0001"
    withIndexName(index1) {
      indexWithIp(index1)

      var df = spark.sql(s"""SELECT client, server FROM ${catalogName}.default.$index1""")
      val ip0 = "192.168.0.10"
      val ip1 = "192.168.0.11"
      val ip2 = "100.10.12.123"
      val ip3 = "2001:db8:3333:4444:5555:6666:7777:8888"
      val ip4 = "2001:db8::1234:5678"
      checkAnswer(df, Seq(Row(ip0, ip2), Row(ip1, ip2), Row(ip3, ip4)))

      df = spark.sql(
        s"""SELECT client, server FROM ${catalogName}.default.$index1 WHERE client = '192.168.0.10'""")
      checkAnswer(df, Seq(Row(ip0, ip2)))

      df = spark.sql(
        s"""SELECT client, server FROM ${catalogName}.default.$index1 WHERE server = '2001:db8::1234:5678'""")
      checkAnswer(df, Seq(Row(ip3, ip4)))
    }
  }
}
