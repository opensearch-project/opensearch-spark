/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.aws

import org.opensearch.client.opensearch._types.Time
import org.opensearch.client.opensearch.core.pit.CreatePitRequest
import org.opensearch.flint.core.storage.OpenSearchClientUtils
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class AWSOpenSearchAccessTestSuite
    extends AnyFlatSpec
    with BeforeAndAfter
    with Matchers
    with AWSOpenSearchSuite {

  it should "Create Pit on AWS OpenSearch" in {
    val indexName = "t00001"
    withIndexName(indexName) {
      simpleIndex(indexName)

      val pit = OpenSearchClientUtils
        .createClient(options)
        .createPit(
          new CreatePitRequest.Builder()
            .targetIndexes(indexName)
            .keepAlive(new Time.Builder().time("10s").build())
            .build())
      pit.pitId() should not be ""
    }
  }
}
