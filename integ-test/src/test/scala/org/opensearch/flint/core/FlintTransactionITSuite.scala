/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core

import java.util.Base64

import scala.collection.JavaConverters.mapAsJavaMapConverter

import org.json4s.{Formats, NoTypeHints}
import org.json4s.native.{JsonMethods, Serialization}
import org.opensearch.flint.OpenSearchTransactionSuite
import org.opensearch.flint.common.metadata.log.{FlintMetadataLogEntry, FlintMetadataLogService}
import org.opensearch.flint.common.metadata.log.FlintMetadataLogEntry.IndexState._
import org.opensearch.flint.core.storage.FlintOpenSearchMetadataLogService
import org.opensearch.index.seqno.SequenceNumbers.{UNASSIGNED_PRIMARY_TERM, UNASSIGNED_SEQ_NO}
import org.scalatest.matchers.should.Matchers

import org.apache.spark.sql.flint.config.FlintSparkConf.DATA_SOURCE_NAME

class FlintTransactionITSuite extends OpenSearchTransactionSuite with Matchers {

  val testFlintIndex = "flint_test_index"
  val testLatestId: String = Base64.getEncoder.encodeToString(testFlintIndex.getBytes)
  var flintMetadataLogService: FlintMetadataLogService = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    val options = openSearchOptions + (DATA_SOURCE_NAME.key -> testDataSourceName)
    flintMetadataLogService = new FlintOpenSearchMetadataLogService(
      new FlintOptions(options.asJava))
  }

  test("empty metadata log entry content") {
    flintMetadataLogService
      .startTransaction(testFlintIndex)
      .initialLog(latest => {
        latest.id shouldBe testLatestId
        latest.state shouldBe EMPTY
        latest.createTime shouldBe 0L
        latest.dataSource shouldBe testDataSourceName
        latest.error shouldBe ""
        true
      })
      .finalLog(latest => latest)
      .commit(_ => {})
  }

  test("should preserve original values when transition") {
    val testCreateTime = 1234567890123L
    createLatestLogEntry(
      FlintMetadataLogEntry(
        id = testLatestId,
        seqNo = UNASSIGNED_SEQ_NO,
        primaryTerm = UNASSIGNED_PRIMARY_TERM,
        createTime = testCreateTime,
        state = ACTIVE,
        dataSource = testDataSourceName,
        error = ""))

    flintMetadataLogService
      .startTransaction(testFlintIndex)
      .initialLog(latest => {
        latest.id shouldBe testLatestId
        latest.createTime shouldBe testCreateTime
        latest.dataSource shouldBe testDataSourceName
        latest.error shouldBe ""
        true
      })
      .transientLog(latest => latest.copy(state = DELETING))
      .finalLog(latest => latest.copy(state = DELETED))
      .commit(latest => {
        latest.id shouldBe testLatestId
        latest.createTime shouldBe testCreateTime
        latest.dataSource shouldBe testDataSourceName
        latest.error shouldBe ""
      })

    latestLogEntry(testLatestId) should (contain("latestId" -> testLatestId) and
      contain("dataSourceName" -> testDataSourceName) and
      contain("error" -> ""))
  }

  test("should transit from initial to final log if initial log is empty") {
    flintMetadataLogService
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

  test("should transit from initial to final log directly if no transient log") {
    flintMetadataLogService
      .startTransaction(testFlintIndex)
      .initialLog(_ => true)
      .finalLog(latest => latest.copy(state = ACTIVE))
      .commit(_ => latestLogEntry(testLatestId) should contain("state" -> "empty"))

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
        createTime = 1234567890123L,
        state = ACTIVE,
        dataSource = testDataSourceName,
        error = ""))

    flintMetadataLogService
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
      flintMetadataLogService
        .startTransaction(testFlintIndex)
        .initialLog(_ => false)
        .transientLog(latest => latest.copy(state = ACTIVE))
        .finalLog(latest => latest)
        .commit(_ => {})
    }

    // Initial empty log should not be changed
    latestLogEntry(testLatestId) should contain("state" -> "empty")
  }

  test("should fail if initial log entry updated by others when updating transient log entry") {
    the[IllegalStateException] thrownBy {
      flintMetadataLogService
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
      flintMetadataLogService
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

  test("should rollback to initial log if transaction operation failed") {
    // Use create index scenario in this test case
    the[IllegalStateException] thrownBy {
      flintMetadataLogService
        .startTransaction(testFlintIndex)
        .initialLog(_ => true)
        .transientLog(latest => latest.copy(state = CREATING))
        .finalLog(latest => latest.copy(state = ACTIVE))
        .commit(_ => throw new RuntimeException("Mock operation error"))
    }

    // Should rollback to initial empty log
    latestLogEntry(testLatestId) should contain("state" -> "empty")
  }

  test("should rollback to initial log if updating final log failed") {
    // Use refresh index scenario in this test case
    createLatestLogEntry(
      FlintMetadataLogEntry(
        id = testLatestId,
        seqNo = UNASSIGNED_SEQ_NO,
        primaryTerm = UNASSIGNED_PRIMARY_TERM,
        createTime = 1234567890123L,
        state = ACTIVE,
        dataSource = testDataSourceName,
        error = ""))

    the[IllegalStateException] thrownBy {
      flintMetadataLogService
        .startTransaction(testFlintIndex)
        .initialLog(_ => true)
        .transientLog(latest => latest.copy(state = REFRESHING))
        .finalLog(_ => throw new RuntimeException("Mock final log error"))
        .commit(_ => {})
    }

    // Should rollback to initial active log
    latestLogEntry(testLatestId) should contain("state" -> "active")
  }

  test(
    "should not necessarily rollback if transaction operation failed but no transient action") {
    // Use create index scenario in this test case
    the[IllegalStateException] thrownBy {
      flintMetadataLogService
        .startTransaction(testFlintIndex)
        .initialLog(_ => true)
        .finalLog(latest => latest.copy(state = ACTIVE))
        .commit(_ => throw new RuntimeException("Mock operation error"))
    }

    // Should rollback to initial empty log
    latestLogEntry(testLatestId) should contain("state" -> "empty")
  }

  test("forceInit translog, even index is deleted before startTransaction") {
    deleteIndex(testMetaLogIndex)
    flintMetadataLogService
      .startTransaction(testFlintIndex, true)
      .initialLog(latest => {
        latest.id shouldBe testLatestId
        latest.state shouldBe EMPTY
        latest.createTime shouldBe 0L
        latest.dataSource shouldBe testDataSourceName
        latest.error shouldBe ""
        true
      })
      .finalLog(latest => latest)
      .commit(_ => {})

    implicit val formats: Formats = Serialization.formats(NoTypeHints)
    (JsonMethods.parse(indexMapping()) \ "properties" \ "sessionId" \ "type")
      .extract[String] should equal("keyword")
  }

  test("should fail if index is deleted before initial operation") {
    the[IllegalStateException] thrownBy {
      flintMetadataLogService
        .startTransaction(testFlintIndex)
        .initialLog(latest => {
          deleteIndex(testMetaLogIndex)
          true
        })
        .transientLog(latest => latest.copy(state = CREATING))
        .finalLog(latest => latest.copy(state = ACTIVE))
        .commit(_ => {})
    }
  }

  test("should fail if index is deleted before transient operation") {
    the[IllegalStateException] thrownBy {
      flintMetadataLogService
        .startTransaction(testFlintIndex)
        .initialLog(latest => true)
        .transientLog(latest => {
          deleteIndex(testMetaLogIndex)
          latest.copy(state = CREATING)
        })
        .finalLog(latest => latest.copy(state = ACTIVE))
        .commit(_ => {})
    }
  }

  test("should fail if index is deleted before final operation") {
    the[IllegalStateException] thrownBy {
      flintMetadataLogService
        .startTransaction(testFlintIndex)
        .initialLog(latest => true)
        .transientLog(latest => { latest.copy(state = CREATING) })
        .finalLog(latest => {
          deleteIndex(testMetaLogIndex)
          latest.copy(state = ACTIVE)
        })
        .commit(_ => {})
    }
  }
}
