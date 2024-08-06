/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.opensearch.catalog

import org.apache.spark.opensearch.table.OpenSearchCatalogSuite
import org.apache.spark.sql.{AnalysisException, Row}

class OpenSearchCatalogITSuite extends OpenSearchCatalogSuite {
  test("Load single index as table") {
    val indexName = "t0001"
    withIndexName(indexName) {
      simpleIndex(indexName)
      val df = spark.sql(s"""
        SELECT accountId, eventName, eventSource
        FROM ${catalogName}.default.$indexName""")

      assert(df.count() == 1)
      checkAnswer(df, Row("123", "event", "source"))
    }
  }

  test("Describe single index as table") {
    val indexName = "t0001"
    withIndexName(indexName) {
      simpleIndex(indexName)
      val df = spark.sql(s"""
        DESC ${catalogName}.default.$indexName""")

      assert(df.count() == 6)
      checkAnswer(
        df,
        Seq(
          Row("# Partitioning", "", ""),
          Row("", "", ""),
          Row("Not partitioned", "", ""),
          Row("accountId", "string", ""),
          Row("eventName", "string", ""),
          Row("eventSource", "string", "")))
    }
  }

  test("Failed to write value to readonly table") {
    val indexName = "t0001"
    withIndexName(indexName) {
      simpleIndex(indexName)
      val exception = intercept[AnalysisException] {
        spark.sql(s"""
          INSERT INTO ${catalogName}.default.$indexName VALUES ('234', 'event-1', 'source-1')""")
      }
      assert(
        exception.getMessage.contains(s"Table $indexName does not support append in batch mode."))
    }
  }

  test("Failed to delete value from readonly table") {
    val indexName = "t0001"
    withIndexName(indexName) {
      simpleIndex(indexName)
      val exception = intercept[AnalysisException] {
        spark.sql(s"DELETE FROM ${catalogName}.default.$indexName WHERE accountId = '234'")
      }
      assert(exception.getMessage.contains(s"Table does not support deletes: $indexName"))
    }
  }

  test("Failed to overwrite value of readonly table") {
    val indexName = "t0001"
    withIndexName(indexName) {
      simpleIndex(indexName)
      val exception = intercept[AnalysisException] {
        spark.sql(s"""
          INSERT OVERWRITE TABLE ${catalogName}.default.$indexName VALUES ('234', 'event-1', 'source-1')""")
      }
      assert(
        exception.getMessage.contains(
          s"Table $indexName does not support truncate in batch mode."))
    }
  }

  test("Load index wildcard expression as table") {
    val indexName1 = "t0001"
    val indexName2 = "t0002"
    withIndexName(indexName1) {
      withIndexName(indexName2) {
        simpleIndex(indexName1)
        simpleIndex(indexName2)
        val df = spark.sql(s"""
        SELECT accountId, eventName, eventSource
        FROM ${catalogName}.default.`t*`""")

        assert(df.count() == 2)
        checkAnswer(df, Seq(Row("123", "event", "source"), Row("123", "event", "source")))
      }
    }
  }

  test("Load comma seperated index expression as table") {
    val indexName1 = "t0001"
    val indexName2 = "t0002"
    withIndexName(indexName1) {
      withIndexName(indexName2) {
        simpleIndex(indexName1)
        simpleIndex(indexName2)
        val df = spark.sql(s"""
        SELECT accountId, eventName, eventSource
        FROM ${catalogName}.default.`t0001,t0002`""")

        assert(df.count() == 2)
        checkAnswer(df, Seq(Row("123", "event", "source"), Row("123", "event", "source")))
      }
    }
  }
}
