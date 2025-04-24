/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.opensearch.index

import scala.collection.JavaConverters._

import org.opensearch.flint.core.FlintOptions

import org.apache.spark.opensearch.table.OpenSearchCatalogSuite
import org.apache.spark.sql.OSClient

class OpenSearchIndexITSuite extends OpenSearchCatalogSuite {

  test("Create index with malformed settings should fail") {
    val osClient = new OSClient(new FlintOptions(openSearchOptions.asJava))

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

    // invalid JSON
    val settings = """{
                        |  "index": {
                        |    "refresh_interval": "1s"
                        |}""".stripMargin

    // should throw error
    osClient.createIndex("test_index", mappings, settings)
  }
}
