/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import java.util.Base64

import org.opensearch.flint.OpenSearchTransactionSuite
import org.opensearch.flint.spark.FlintSpark.RefreshMode.{FULL, INCREMENTAL}
import org.opensearch.flint.spark.skipping.FlintSparkSkippingIndex.getSkippingIndexName
import org.scalatest.matchers.should.Matchers

class FlintSparkTransactionITSuite
    extends FlintSparkSuite
    with OpenSearchTransactionSuite
    with Matchers {

  /** Test table and index name */
  private val testTable = "spark_catalog.default.test"
  private val testFlintIndex = getSkippingIndexName(testTable)
  private val testLatestId: String = Base64.getEncoder.encodeToString(testFlintIndex.getBytes)

  override def beforeAll(): Unit = {
    super.beforeAll()
    createPartitionedTable(testTable)
  }

  override def afterEach(): Unit = {
    super.afterEach()
    flint.deleteIndex(testFlintIndex)
  }

  test("create index") {
    flint
      .skippingIndex()
      .onTable(testTable)
      .addPartitions("year", "month")
      .create()
    latestLogEntry(testLatestId) should contain("state" -> "active")
  }

  test("manual refresh index") {
    flint
      .skippingIndex()
      .onTable(testTable)
      .addPartitions("year", "month")
      .create()
    flint.refreshIndex(testFlintIndex, FULL)

    latestLogEntry(testLatestId) should contain("state" -> "active")
  }

  test("incremental refresh index") {
    flint
      .skippingIndex()
      .onTable(testTable)
      .addPartitions("year", "month")
      .create()
    flint.refreshIndex(testFlintIndex, INCREMENTAL)
    latestLogEntry(testLatestId) should contain("state" -> "refreshing")
  }

  test("delete index") {
    flint
      .skippingIndex()
      .onTable(testTable)
      .addPartitions("year", "month")
      .create()
    flint.deleteIndex(testFlintIndex)

    latestLogEntry(testLatestId) should contain("state" -> "deleted")
  }
}
