/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import org.opensearch.flint.OpenSearchTransactionSuite
import org.opensearch.flint.core.FlintVersion
import org.opensearch.flint.core.metadata.FlintMetadata
import org.scalatest.matchers.should.Matchers

import org.apache.spark.sql.{DataFrame, SparkSession}

class FlintSparkTransactionITSuite
    extends FlintSparkSuite
    with OpenSearchTransactionSuite
    with Matchers {

  /** Test Flint index implementation */
  class FlintSparkFakeIndex extends FlintSparkIndex {
    override val kind: String = "fake"

    override val options: FlintSparkIndexOptions = FlintSparkIndexOptions.empty

    override def name(): String = "fake_index"

    override def metadata(): FlintMetadata =
      new FlintMetadata(FlintVersion.current(), name(), kind, "source", indexSettings = None)

    override def build(spark: SparkSession, df: Option[DataFrame]): DataFrame = {
      null
    }
  }

  test("create and refresh index") {

    flint.createIndex(new FlintSparkFakeIndex)
  }
}
