/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.storage

import org.scalatest.Ignore
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/*
TODO: Passes when it's run by itself, but always fail when run in sbt test.
Tracked in https://github.com/opensearch-project/opensearch-spark/issues/1097
 */
@Ignore
class OpenSearchClientUtilsSuite extends AnyFlatSpec with Matchers {

  "sanitizeIndexName" should "percent-encode invalid OpenSearch index name characters and lowercase all characters" in {
    val indexName = "TEST :\"+/\\|?#><"
    val sanitizedIndexName = OpenSearchClientUtils.sanitizeIndexName(indexName)
    sanitizedIndexName shouldBe "test%20%3a%22%2b%2f%5c%7c%3f%23%3e%3c"
  }
}
