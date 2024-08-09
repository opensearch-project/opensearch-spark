/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import org.mockito.Mockito.{never, reset, times, verify, when, RETURNS_DEEP_STUBS}
import org.opensearch.flint.common.metadata.log.FlintMetadataLogService
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
    reset(mockFlintClient, mockFlintMetadataLogService)
  }

  test("execute transaction without force initialization when index exists") {
    when(mockFlintClient.exists(testIndex)).thenReturn(true)
    val result =
      transactionSupport
        .withTransaction[Boolean](testIndex, testOpName) { _ => true }

    result shouldBe Some(true)
    verify(mockFlintMetadataLogService, times(1)).startTransaction(testIndex, false)
  }

  test("execute transaction with force initialization when index exists") {
    when(mockFlintClient.exists(testIndex)).thenReturn(true)
    val result =
      transactionSupport
        .withTransaction[Boolean](testIndex, testOpName, forceInit = true) { _ => true }

    result shouldBe Some(true)
    verify(mockFlintMetadataLogService, times(1)).startTransaction(testIndex, true)
  }

  test("bypass transaction without force initialization when index does not exist") {
    when(mockFlintClient.exists(testIndex)).thenReturn(false)
    val result =
      transactionSupport
        .withTransaction[Boolean](testIndex, testOpName) { _ => true }

    result shouldBe None
  }

  test("execute transaction with force initialization even if index does not exist") {
    when(mockFlintClient.exists(testIndex)).thenReturn(false)
    val result =
      transactionSupport
        .withTransaction[Boolean](testIndex, testOpName, forceInit = true) { _ => true }

    result shouldBe Some(true)
    verify(mockFlintMetadataLogService, times(1)).startTransaction(testIndex, true)
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
}
