/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.opensearch.catalog

import org.opensearch.flint.OpenSearchSuite

import org.apache.spark.FlintSuite
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.streaming.StreamTest

class OpenSearchCatalogITSuite
    extends QueryTest
    with StreamTest
    with FlintSuite
    with OpenSearchSuite {

  private val catalogName = "dev"

  override def beforeAll(): Unit = {
    super.beforeAll()

    spark.conf.set(
      s"spark.sql.catalog.${catalogName}",
      "org.apache.spark.opensearch.catalog.OpenSearchCatalog")
    spark.conf.set(s"spark.sql.catalog.${catalogName}.opensearch.port", s"$openSearchPort")
    spark.conf.set(s"spark.sql.catalog.${catalogName}.opensearch.host", openSearchHost)
  }

  test("Load single index as table") {
    val indexName = "t0001"
    withIndexName(indexName) {
      simpleIndex(indexName)
      val df = spark.sql(s"""
        SELECT accountId, eventName, eventSource
        FROM ${catalogName}.default.${indexName}""")

      assert(df.count() == 1)
      checkAnswer(df, Row("123", "event", "source"))
    }
  }

  test("Load index wildcard expression as table") {
    val indexName = "t0001"
    withIndexName(indexName) {
      simpleIndex(indexName)
      val df = spark.sql(s"""
        SELECT accountId, eventName, eventSource
        FROM ${catalogName}.default.`t*`""")

      assert(df.count() == 1)
      checkAnswer(df, Row("123", "event", "source"))
    }
  }

  // FIXME, enable it when add partition info into OpenSearchTable.
  ignore("Load comma seperated index expression as table") {
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
