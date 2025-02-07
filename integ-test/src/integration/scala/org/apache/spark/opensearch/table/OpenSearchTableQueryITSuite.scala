/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.opensearch.table

import org.opensearch.flint.spark.ppl.FlintPPLSuite

import org.apache.spark.FlintDataSourceV2ITSuite
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.jdbc.JDBCV2Suite

/**
 * Test queries on OpenSearch Table
 */
class OpenSearchTableQueryITSuite
    extends FlintDataSourceV2ITSuite
    with OpenSearchCatalogSuite
    with FlintPPLSuite {
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

  test("Query multi-field index - Exact match on multi-field is pushed down") {
    val indexName = "t0001"
    val table = s"${catalogName}.default.$indexName"
    withIndexName(indexName) {
      indexMultiFields(indexName)
      // Validate that an exact equality query on the multi-field 'aTextString'
      // is pushed down to OpenSearch and returns the matching document.
      var df =
        spark.sql(s"""SELECT id FROM $table WHERE aTextString = "Treviso-Sant'Angelo Airport" """)
      checkPushedInfo(df, "aTextString IS NOT NULL, aTextString = 'Treviso-Sant'Angelo Airport'")
      checkAnswer(df, Seq(Row(1)))

      // Validate that an equality query on 'aTextString' with a non-exact match returns no results.
      df = spark.sql(s"""SELECT id FROM $table WHERE aTextString = "Airport" """)
      checkPushedInfo(df, "aTextString IS NOT NULL, aTextString = 'Airport'")
      checkAnswer(df, Seq())
    }
  }

  test("Query text fields index - Combined conditions on text and keyword fields") {
    val indexName = "t0001"
    val table = s"${catalogName}.default.$indexName"
    withIndexName(indexName) {
      indexMultiFields(indexName)

      // Validate that the condition on the text field 'aText', which is evaluated by Spark
      // without push down to OpenSearch, returns the expected document
      var df =
        spark.sql(s"""SELECT id FROM $table WHERE aText = "Treviso-Sant'Angelo Airport" """)
      checkPushedInfo(df, "aText IS NOT NULL")
      checkAnswer(df, Seq(Row(1)))

      // Validate that the condition on the text field 'aText', which is evaluated by Spark,
      // aString is push down to OpenSearch returns the expected document when both conditions are met.
      df = spark.sql(
        s"""SELECT id FROM $table WHERE aText = "Treviso-Sant'Angelo Airport" AND aString = "OpenSearch-Air" """)
      checkPushedInfo(df, "aText IS NOT NULL, aString IS NOT NULL, aString = 'OpenSearch-Air'")
      checkAnswer(df, Seq(Row(1)))

      // Validate that a query with a full string equality condition on 'aText'
      // that does not exactly match returns no results.
      df = spark.sql(s"""SELECT id FROM $table WHERE aText = "Airport" """)
      checkAnswer(df, Seq())
    }
  }
}
