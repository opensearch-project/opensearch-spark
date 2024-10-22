/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core

import java.util.{Base64, Optional}

import scala.collection.JavaConverters._

import org.opensearch.flint.OpenSearchTransactionSuite
import org.opensearch.flint.common.metadata.log.{FlintMetadataLog, FlintMetadataLogEntry, FlintMetadataLogService, OptimisticTransaction}
import org.opensearch.flint.common.metadata.log.FlintMetadataLogEntry.IndexState._
import org.opensearch.flint.core.metadata.log.DefaultOptimisticTransaction
import org.opensearch.flint.core.metadata.log.FlintMetadataLogServiceBuilder
import org.opensearch.flint.core.storage.FlintOpenSearchMetadataLog
import org.opensearch.flint.core.storage.FlintOpenSearchMetadataLogService
import org.opensearch.index.seqno.SequenceNumbers.{UNASSIGNED_PRIMARY_TERM, UNASSIGNED_SEQ_NO}
import org.scalatest.matchers.should.Matchers

import org.apache.spark.sql.flint.config.FlintSparkConf.DATA_SOURCE_NAME

class FlintMetadataLogITSuite extends OpenSearchTransactionSuite with Matchers {

  val testFlintIndex = "flint_test_index"
  val testLatestId: String = Base64.getEncoder.encodeToString(testFlintIndex.getBytes)
  val testTimestamp = 1234567890123L
  val flintMetadataLogEntry = FlintMetadataLogEntry(
    testLatestId,
    testTimestamp,
    testTimestamp,
    testTimestamp,
    ACTIVE,
    Map("seqNo" -> UNASSIGNED_SEQ_NO, "primaryTerm" -> UNASSIGNED_PRIMARY_TERM),
    "",
    Map("dataSourceName" -> testDataSourceName))

  var flintMetadataLogService: FlintMetadataLogService = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    val options = openSearchOptions + (DATA_SOURCE_NAME.key -> testDataSourceName)
    val flintOptions = new FlintOptions(options.asJava)
    flintMetadataLogService = new FlintOpenSearchMetadataLogService(flintOptions)
  }

  test("should build metadata log service") {
    val customOptions =
      openSearchOptions + (FlintOptions.CUSTOM_FLINT_METADATA_LOG_SERVICE_CLASS -> "org.opensearch.flint.core.TestMetadataLogService")
    val customFlintOptions = new FlintOptions(customOptions.asJava)
    val customFlintMetadataLogService =
      FlintMetadataLogServiceBuilder.build(customFlintOptions)
    customFlintMetadataLogService shouldBe a[TestMetadataLogService]
  }

  test("should fail to build metadata log service if class name doesn't exist") {
    val options =
      openSearchOptions + (FlintOptions.CUSTOM_FLINT_METADATA_LOG_SERVICE_CLASS -> "dummy")
    val flintOptions = new FlintOptions(options.asJava)
    the[RuntimeException] thrownBy {
      FlintMetadataLogServiceBuilder.build(flintOptions)
    }
  }

  test("should fail to start transaction if metadata log index doesn't exists") {
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
    latest.get.createTime shouldBe testTimestamp
    latest.get.error shouldBe ""
    latest.get.lastRefreshStartTime shouldBe testTimestamp
    latest.get.lastRefreshCompleteTime shouldBe testTimestamp
    latest.get.properties.get("dataSourceName").get shouldBe testDataSourceName
  }

  test("should not get index metadata log if not exist") {
    val options = openSearchOptions + (DATA_SOURCE_NAME.key -> "non-exist-datasource")
    val flintMetadataLogService =
      new FlintOpenSearchMetadataLogService(new FlintOptions(options.asJava))
    val metadataLog = flintMetadataLogService.getIndexMetadataLog(testFlintIndex)
    metadataLog.isPresent shouldBe false
  }

  test("should update timestamp when record heartbeat") {
    val refreshingLogEntry = flintMetadataLogEntry.copy(state = REFRESHING)
    createLatestLogEntry(refreshingLogEntry)
    val updateTimeBeforeHeartbeat =
      latestLogEntry(testLatestId).get("lastUpdateTime").get.asInstanceOf[Long]
    flintMetadataLogService.recordHeartbeat(testFlintIndex)
    latestLogEntry(testLatestId)
      .get("lastUpdateTime")
      .get
      .asInstanceOf[Long] should be > updateTimeBeforeHeartbeat
  }

  test("should fail when record heartbeat if index not refreshing") {
    createLatestLogEntry(flintMetadataLogEntry)
    the[IllegalStateException] thrownBy {
      flintMetadataLogService.recordHeartbeat(testFlintIndex)
    }
  }
}

class TestMetadataLogService extends FlintMetadataLogService {
  override def startTransaction[T](
      indexName: String,
      forceInit: Boolean): OptimisticTransaction[T] = {
    val flintOptions = new FlintOptions(Map[String, String]().asJava)
    val metadataLog = new FlintOpenSearchMetadataLog(flintOptions, "", "")
    new DefaultOptimisticTransaction(metadataLog)
  }

  override def startTransaction[T](indexName: String): OptimisticTransaction[T] = {
    startTransaction(indexName, false)
  }

  override def getIndexMetadataLog(
      indexName: String): Optional[FlintMetadataLog[FlintMetadataLogEntry]] = {
    Optional.empty()
  }

  override def recordHeartbeat(indexName: String): Unit = {}
}
