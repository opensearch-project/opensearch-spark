/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core

import java.util.Base64

import scala.collection.JavaConverters.mapAsJavaMapConverter

import org.opensearch.flint.OpenSearchTransactionSuite
import org.opensearch.flint.core.metadata.log.FlintMetadataLogEntry
import org.opensearch.flint.core.metadata.log.FlintMetadataLogEntry.IndexState._
import org.opensearch.flint.core.storage.FlintOpenSearchClient
import org.opensearch.flint.spark.FlintSparkSuite
import org.opensearch.index.seqno.SequenceNumbers.{UNASSIGNED_PRIMARY_TERM, UNASSIGNED_SEQ_NO}
import org.scalatest.matchers.should.Matchers

class FlintOpenSearchTransactionITSuite
    extends FlintSparkSuite
    with OpenSearchTransactionSuite
    with Matchers {

  val testFlintIndex = "flint_test_index"
  val testLatestId: String = Base64.getEncoder.encodeToString(testFlintIndex.getBytes)
  var flintClient: FlintClient = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    flintClient = new FlintOpenSearchClient(new FlintOptions(openSearchOptions.asJava))
  }

  test("should transit from initial to final log if initial log is empty") {
    flintClient
      .startTransaction(testFlintIndex)
      .initialLog(latest => {
        latest.state shouldBe EMPTY
        true
      })
      .transientLog(latest => latest.copy(state = CREATING))
      .finalLog(latest => latest.copy(state = ACTIVE))
      .commit(_ => latestLogEntry(testLatestId) should contain("state" -> "creating"))

    latestLogEntry(testLatestId) should contain("state" -> "active")
  }

  test(
    "should transit from initial to final log if initial is not empty but precondition satisfied") {
    // Create doc first to simulate this scenario
    createLatestLogEntry(
      FlintMetadataLogEntry(
        id = testLatestId,
        seqNo = UNASSIGNED_SEQ_NO,
        primaryTerm = UNASSIGNED_PRIMARY_TERM,
        state = ACTIVE,
        dataSource = "mys3",
        error = ""))

    flintClient
      .startTransaction(testFlintIndex)
      .initialLog(latest => {
        latest.state shouldBe ACTIVE
        true
      })
      .transientLog(latest => latest.copy(state = REFRESHING))
      .finalLog(latest => latest.copy(state = ACTIVE))
      .commit(_ => latestLogEntry(testLatestId) should contain("state" -> "refreshing"))

    latestLogEntry(testLatestId) should contain("state" -> "active")
  }

  test("should exit if initial log entry doesn't meet precondition") {
    the[IllegalStateException] thrownBy {
      flintClient
        .startTransaction(testFlintIndex)
        .initialLog(_ => false)
        .transientLog(latest => latest)
        .finalLog(latest => latest)
        .commit(_ => {})
    }
  }

  test("should fail if initial log entry updated by others when updating transient log entry") {
    the[IllegalStateException] thrownBy {
      flintClient
        .startTransaction(testFlintIndex)
        .initialLog(_ => true)
        .transientLog(latest => {
          // This update will happen first and thus cause version conflict as expected
          updateLatestLogEntry(latest, DELETING)

          latest.copy(state = CREATING)
        })
        .finalLog(latest => latest)
        .commit(_ => {})
    }
  }

  test("should fail if transient log entry updated by others when updating final log entry") {
    the[IllegalStateException] thrownBy {
      flintClient
        .startTransaction(testFlintIndex)
        .initialLog(_ => true)
        .transientLog(latest => {

          latest.copy(state = CREATING)
        })
        .finalLog(latest => latest)
        .commit(latest => {
          // This update will happen first and thus cause version conflict as expected
          updateLatestLogEntry(latest, DELETING)
        })
    }
  }
}
