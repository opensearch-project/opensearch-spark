/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.aws

import org.opensearch.flint.core.model.CreatePitReq
import org.opensearch.flint.core.storage.FlintOpenSearchClient
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

      val pitResp = new FlintOpenSearchClient(options)
        .createPit(CreatePitReq(indexName, "5m"))
      pitResp.pit should not be ""
    }
  }
}
