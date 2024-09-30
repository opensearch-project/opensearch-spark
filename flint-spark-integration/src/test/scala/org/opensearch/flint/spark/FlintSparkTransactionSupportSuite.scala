/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import java.util.Optional
import java.util.function.{Function, Predicate}

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.opensearch.flint.common.metadata.log.{FlintMetadataLog, FlintMetadataLogEntry, FlintMetadataLogService, OptimisticTransaction}
import org.opensearch.flint.common.metadata.log.FlintMetadataLogEntry.IndexState._
import org.opensearch.flint.core.FlintClient
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar.mock

import org.apache.spark.FlintSuite

class FlintSparkTransactionSupportSuite extends FlintSuite with Matchers {

  private val mockFlintClient: FlintClient = mock[FlintClient]
  private val mockFlintMetadataLogService: FlintMetadataLogService = mock[FlintMetadataLogService]
  private val mockTransaction = mock[OptimisticTransaction[_]]
  private val mockLogEntry = mock[FlintMetadataLogEntry]
  private val testIndex = "test_index"
  private val testOpName = "test operation"

  /** Creating a fake FlintSparkTransactionSupport subclass for test */
  private val transactionSupport = new FlintSparkTransactionSupport {
    override protected def flintClient: FlintClient = mockFlintClient
    override protected def flintMetadataLogService: FlintMetadataLogService =
      mockFlintMetadataLogService
  }

  override protected def beforeEach(): Unit = {
    super.beforeEach()

    val logEntry = mock[FlintMetadataLog[FlintMetadataLogEntry]]
    when(logEntry.getLatest).thenReturn(Optional.of(mockLogEntry))
    when(mockFlintMetadataLogService.getIndexMetadataLog(testIndex))
      .thenReturn(Optional.of(logEntry))
    when(mockFlintMetadataLogService.startTransaction(any[String]))
      .thenAnswer((_: InvocationOnMock) => mockTransaction)

    // Mock transaction method chain
    when(mockTransaction.initialLog(any[Predicate[FlintMetadataLogEntry]]))
      .thenAnswer((_: InvocationOnMock) => mockTransaction)
    when(mockTransaction.finalLog(any[Function[FlintMetadataLogEntry, FlintMetadataLogEntry]]))
      .thenAnswer((_: InvocationOnMock) => mockTransaction)
  }

  override protected def afterEach(): Unit = {
    reset(mockFlintClient, mockFlintMetadataLogService, mockTransaction, mockLogEntry)
    super.afterEach()
  }

  test("execute transaction") {
    assertIndexOperation()
      .withForceInit(false)
      .withResult("test")
      .whenIndexDataExists()
      .expectResult("test")
      .verifyTransaction(forceInit = false)
      .verifyLogEntryCleanup(false)
  }

  test("execute fore init transaction") {
    assertIndexOperation()
      .withForceInit(true)
      .withResult("test")
      .whenIndexDataExists()
      .expectResult("test")
      .verifyTransaction(forceInit = true)
      .verifyLogEntryCleanup(false)
  }

  Seq(EMPTY, CREATING, VACUUMING).foreach { indexState =>
    test(s"execute transaction when corrupted index in $indexState") {
      assertIndexOperation()
        .withForceInit(false)
        .withResult("test")
        .withIndexState(indexState)
        .whenIndexDataNotExist()
        .expectResult("test")
        .verifyTransaction(forceInit = false)
        .verifyLogEntryCleanup(false)
    }
  }

  Seq(EMPTY, CREATING, VACUUMING).foreach { indexState =>
    test(s"execute force init transaction if corrupted index in $indexState") {
      assertIndexOperation()
        .withForceInit(true)
        .withResult("test")
        .withIndexState(indexState)
        .whenIndexDataNotExist()
        .expectResult("test")
        .verifyTransaction(forceInit = true)
        .verifyLogEntryCleanup(false)
    }
  }

  Seq(ACTIVE, UPDATING, REFRESHING, DELETING, DELETED, RECOVERING, FAILED).foreach { indexState =>
    test(s"clean up log entry and bypass transaction when corrupted index in $indexState") {
      assertIndexOperation()
        .withForceInit(false)
        .withResult("test")
        .withIndexState(indexState)
        .whenIndexDataNotExist()
        .expectNoResult()
        .verifyLogEntryCleanup(true)
    }
  }

  Seq(ACTIVE, UPDATING, REFRESHING, DELETING, DELETED, RECOVERING, FAILED).foreach { indexState =>
    test(
      s"clean up log entry and execute force init transaction when corrupted index in $indexState") {
      assertIndexOperation()
        .withForceInit(true)
        .withResult("test")
        .withIndexState(indexState)
        .whenIndexDataNotExist()
        .expectResult("test")
        .verifyLogEntryCleanup(true)
        .verifyTransaction(forceInit = true)
        .verifyLogEntryCleanup(true)
    }
  }

  test("propagate original exception thrown within transaction") {
    the[RuntimeException] thrownBy {
      when(mockFlintClient.exists(testIndex)).thenReturn(true)

      transactionSupport.withTransaction[Unit](testIndex, testOpName) { _ =>
        val rootCause = new IllegalArgumentException("Fake root cause")
        val cause = new RuntimeException("Fake cause", rootCause)
        throw cause
      }
    } should have message "Fake cause"
  }

  private def assertIndexOperation(): FlintIndexAssertion = new FlintIndexAssertion

  class FlintIndexAssertion {
    private var forceInit: Boolean = false
    private var expectedResult: Option[String] = None

    def withForceInit(forceInit: Boolean): FlintIndexAssertion = {
      this.forceInit = forceInit
      this
    }

    def withResult(expectedResult: String): FlintIndexAssertion = {
      this.expectedResult = Some(expectedResult)
      this
    }

    def withIndexState(expectedState: IndexState): FlintIndexAssertion = {
      when(mockLogEntry.state).thenReturn(expectedState)
      this
    }

    def whenIndexDataExists(): FlintIndexAssertion = {
      when(mockFlintClient.exists(testIndex)).thenReturn(true)
      this
    }

    def whenIndexDataNotExist(): FlintIndexAssertion = {
      when(mockFlintClient.exists(testIndex)).thenReturn(false)
      this
    }

    def verifyLogEntryCleanup(cleanup: Boolean): FlintIndexAssertion = {
      verify(mockTransaction, if (cleanup) times(1) else never())
        .commit(any())
      this
    }

    def verifyTransaction(forceInit: Boolean): FlintIndexAssertion = {
      verify(mockFlintMetadataLogService, times(1)).startTransaction(testIndex, forceInit)
      this
    }

    def expectResult(expectedResult: String): FlintIndexAssertion = {
      val result = transactionSupport.withTransaction[String](testIndex, testOpName, forceInit) {
        _ => expectedResult
      }
      result shouldBe Some(expectedResult)
      this
    }

    def expectNoResult(): FlintIndexAssertion = {
      val result = transactionSupport.withTransaction[String](testIndex, testOpName, forceInit) {
        _ => expectedResult.getOrElse("")
      }
      result shouldBe None
      this
    }
  }
}
