/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import org.mockito.Mockito.{times, verify}
import org.opensearch.flint.common.metadata.log.FlintMetadataLogService
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar.mock

import org.apache.spark.FlintSuite
import org.apache.spark.internal.Logging

class FlintSparkTransactionSupportSuite extends FlintSuite with Matchers {

  private val mockFlintMetadataLogService: FlintMetadataLogService = mock[FlintMetadataLogService]
  private val testIndex = "test_index"
  private val testOpName = "test operation"

  /** Creating a fake FlintSparkTransactionSupport subclass for test */
  private val transactionSupport = new FlintSparkTransactionSupport with Logging {
    override protected def flintMetadataLogService: FlintMetadataLogService =
      mockFlintMetadataLogService
  }

  test("with transaction without force initialization") {
    transactionSupport.withTransaction[Unit](testIndex, testOpName) { _ => }

    verify(mockFlintMetadataLogService, times(1)).startTransaction(testIndex, false)
  }

  test("with transaction with force initialization") {
    transactionSupport.withTransaction[Unit](testIndex, testOpName, forceInit = true) { _ => }

    verify(mockFlintMetadataLogService, times(1)).startTransaction(testIndex, true)
  }

  test("should throw exception with nested exception message") {
    the[IllegalStateException] thrownBy {
      transactionSupport.withTransaction[Unit](testIndex, testOpName) { _ =>
        throw new IllegalArgumentException("Fake exception")
      }
    } should have message
      "Failed to execute index operation [test operation] caused by: Fake exception"
  }

  test("should throw exception with root cause exception message") {
    the[IllegalStateException] thrownBy {
      transactionSupport.withTransaction[Unit](testIndex, testOpName) { _ =>
        val rootCause = new IllegalArgumentException("Fake root cause")
        val cause = new RuntimeException("message ignored", rootCause)
        throw new IllegalStateException("message ignored", cause)
      }
    } should have message
      "Failed to execute index operation [test operation] caused by: Fake root cause"
  }
}
