/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import java.util.Optional

import org.mockito.Mockito._
import org.opensearch.flint.common.metadata.log.{FlintMetadataLog, FlintMetadataLogEntry, FlintMetadataLogService}
import org.opensearch.flint.core.FlintClient
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar.mock

import org.apache.spark.FlintSuite

class FlintSparkTransactionSupportSuite extends FlintSuite with Matchers {

  private val mockFlintClient: FlintClient = mock[FlintClient]
  private val mockFlintMetadataLogService: FlintMetadataLogService =
    mock[FlintMetadataLogService](RETURNS_DEEP_STUBS)
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
    when(logEntry.getLatest).thenReturn(Optional.of(mock[FlintMetadataLogEntry]))
    when(mockFlintMetadataLogService.getIndexMetadataLog(testIndex))
      .thenReturn(Optional.of(logEntry))
  }

  override protected def afterEach(): Unit = {
    reset(mockFlintClient, mockFlintMetadataLogService)
    super.afterEach()
  }

  test("execute transaction without force initialization") {
    assertIndexOperation()
      .withForceInit(false)
      .withResult("test")
      .whenIndexDataExists()
      .expectResult("test")
      .verifyTransaction(forceInit = false)
  }

  test("execute transaction with force initialization") {
    assertIndexOperation()
      .withForceInit(true)
      .withResult("test")
      .whenIndexDataExists()
      .expectResult("test")
      .verifyTransaction(forceInit = true)
  }

  test("bypass transaction without force initialization when index corrupted") {
    assertIndexOperation()
      .withForceInit(false)
      .withResult("test")
      .whenIndexDataNotExist()
      .expectNoResult()
  }

  test("execute transaction with force initialization even if index corrupted") {
    assertIndexOperation()
      .withForceInit(true)
      .withResult("test")
      .whenIndexDataNotExist()
      .expectResult("test")
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

    def whenIndexDataExists(): FlintIndexAssertion = {
      when(mockFlintClient.exists(testIndex)).thenReturn(true)
      this
    }

    def whenIndexDataNotExist(): FlintIndexAssertion = {
      when(mockFlintClient.exists(testIndex)).thenReturn(false)
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

    def expectNoResult(): Unit = {
      val result = transactionSupport.withTransaction[String](testIndex, testOpName, forceInit) {
        _ => expectedResult.getOrElse("")
      }
      result shouldBe None
    }
  }
}
