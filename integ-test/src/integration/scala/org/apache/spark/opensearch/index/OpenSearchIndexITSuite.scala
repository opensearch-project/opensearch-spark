/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.opensearch.index

import scala.collection.JavaConverters._

import org.opensearch.flint.core.FlintOptions

import org.apache.spark.opensearch.table.OpenSearchCatalogSuite
import org.apache.spark.sql.{FlintJob, OSClient}

class OpenSearchIndexITSuite extends OpenSearchCatalogSuite {

  var osClient: OSClient = _
  val indexName = "test_index"

  override def beforeAll(): Unit = {
    super.beforeAll()
    osClient = new OSClient(new FlintOptions(openSearchOptions.asJava))
  }

  protected override def afterEach(): Unit = {
    super.afterEach()
    deleteTestIndex(indexName)
  }

  test("FlintJobExecutor creating index with malformed settings should fail") {
    val mappings =
      """{
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

    // invalid JSON
    val settings =
      """{
        |  "index": {
        |    "refresh_interval": "1s"
        |}""".stripMargin

    val result = FlintJob.createResultIndex(osClient, indexName, mappings, settings)
    result match {
      case Left(str) => assert(str == s"Failed to create result index $indexName")
      case Right(_) => fail("passing in invalid settings did not fail")
    }
  }

  test("FlintJobExecutor creating index with malformed mappings should fail") {
    val osClient = new OSClient(new FlintOptions(openSearchOptions.asJava))

    // invalid JSON
    val mappings =
      """{
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
        |}""".stripMargin

    val settings =
      """{
        |  "index": {
        |    "refresh_interval": "1s"
        |  }
        |}""".stripMargin

    val result = FlintJob.createResultIndex(osClient, indexName, mappings, settings)
    result match {
      case Left(str) => assert(str == s"Failed to create result index $indexName")
      case Right(_) => fail("passing in invalid mappings did not fail")
    }
  }
}
