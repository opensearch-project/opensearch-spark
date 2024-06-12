/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core

import java.util.Base64

import scala.collection.JavaConverters._

import org.mockito.Mockito.when
import org.opensearch.flint.OpenSearchTransactionSuite
import org.opensearch.flint.core.metadata.FlintMetadata
import org.opensearch.flint.core.metadata.log.FlintMetadataLogEntry
import org.opensearch.flint.core.metadata.log.FlintMetadataLogEntry.IndexState._
import org.opensearch.flint.core.metadata.log.FlintMetadataLogService
import org.opensearch.flint.core.storage.FlintOpenSearchClient
import org.opensearch.flint.core.storage.FlintOpenSearchMetadataLogService
import org.opensearch.index.seqno.SequenceNumbers.{UNASSIGNED_PRIMARY_TERM, UNASSIGNED_SEQ_NO}
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar.mock

import org.apache.spark.sql.flint.config.FlintSparkConf.DATA_SOURCE_NAME

class FlintMetadataLogITSuite extends OpenSearchTransactionSuite with Matchers {

  val testFlintIndex = "flint_test_index"
  val testLatestId: String = Base64.getEncoder.encodeToString(testFlintIndex.getBytes)
  val testCreateTime = 1234567890123L
  val flintMetadataLogEntry = FlintMetadataLogEntry(
    id = testLatestId,
    seqNo = UNASSIGNED_SEQ_NO,
    primaryTerm = UNASSIGNED_PRIMARY_TERM,
    createTime = testCreateTime,
    state = ACTIVE,
    dataSource = testDataSourceName,
    error = "")

  var flintMetadataLogService: FlintMetadataLogService = _
  var flintClient: FlintClient = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    val options = openSearchOptions + (DATA_SOURCE_NAME.key -> testDataSourceName)
    val flintOptions = new FlintOptions(options.asJava)
    flintMetadataLogService = new FlintOpenSearchMetadataLogService(flintOptions)
    flintClient = new FlintOpenSearchClient(flintOptions, flintMetadataLogService)
  }

  test("should fail if metadata log index doesn't exists") {
    val options = openSearchOptions + (DATA_SOURCE_NAME.key -> "non-exist-datasource")
    val flintMetadataLogService =
      new FlintOpenSearchMetadataLogService(new FlintOptions(options.asJava))

    the[IllegalStateException] thrownBy {
      flintMetadataLogService.startTransaction(testFlintIndex)
    }
  }

  test("should get index metadata log without log entry") {
    val metadataLog = flintMetadataLogService.getIndexMetadataLog(testFlintIndex)
    metadataLog.isPresent shouldBe true
    metadataLog.get.getLatest shouldBe empty
  }

  test("should get index metadata log with log entry") {
    createLatestLogEntry(flintMetadataLogEntry)
    val metadataLog = flintMetadataLogService.getIndexMetadataLog(testFlintIndex)
    metadataLog.isPresent shouldBe true

    val latest = metadataLog.get.getLatest
    latest.isPresent shouldBe true
    latest.get.id shouldBe testLatestId
    latest.get.createTime shouldBe testCreateTime
    latest.get.dataSource shouldBe testDataSourceName
    latest.get.error shouldBe ""
  }

  test("should not get index metadata log if not exist") {
    val options = openSearchOptions + (DATA_SOURCE_NAME.key -> "non-exist-datasource")
    val flintMetadataLogService =
      new FlintOpenSearchMetadataLogService(new FlintOptions(options.asJava))
    val metadataLog = flintMetadataLogService.getIndexMetadataLog(testFlintIndex)
    metadataLog.isPresent shouldBe false
  }

  test("should initialize index metadata log if forceInit") {
    val options = openSearchOptions + (DATA_SOURCE_NAME.key -> "non-exist-datasource")
    val flintMetadataLogService =
      new FlintOpenSearchMetadataLogService(new FlintOptions(options.asJava))
    val metadataLog = flintMetadataLogService.getIndexMetadataLog(testFlintIndex, true)
    metadataLog.isPresent shouldBe true
  }

  test("should get index metadata with latest log entry") {
    val metadata = mock[FlintMetadata]
    when(metadata.getContent).thenReturn("{}")
    when(metadata.indexSettings).thenReturn(None)
    when(metadata.latestLogEntry).thenReturn(Some(flintMetadataLogEntry))

    flintClient.createIndex(testFlintIndex, metadata)
    createLatestLogEntry(flintMetadataLogEntry)

    val latest = flintClient.getIndexMetadata(testFlintIndex).latestLogEntry
    latest.isDefined shouldBe true
    latest.get.id shouldBe testLatestId
    latest.get.createTime shouldBe testCreateTime
    latest.get.dataSource shouldBe testDataSourceName
    latest.get.error shouldBe ""

    deleteTestIndex(testFlintIndex)
  }

  test("should get index metadata without log entry") {
    val metadata = mock[FlintMetadata]
    when(metadata.getContent).thenReturn("{}")
    when(metadata.indexSettings).thenReturn(None)
    flintClient.createIndex(testFlintIndex, metadata)

    flintClient.getIndexMetadata(testFlintIndex).latestLogEntry shouldBe empty

    deleteTestIndex(testFlintIndex)
  }
}
