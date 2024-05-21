/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import org.mockito.Mockito.{times, verify}
import org.opensearch.flint.core.FlintClient
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar.mock

import org.apache.spark.FlintSuite
import org.apache.spark.internal.Logging

class FlintSparkTransactionSupportSuite extends FlintSuite with Matchers {

  private val mockFlintClient: FlintClient = mock[FlintClient]
  private val testDataSource: String = "myglue"
  private val testIndex = "test_index"
  private val testOpName = "test operation"

  // Creating a fake FlintSparkTransactionSupport for test
  private val transactionSupport = new FlintSparkTransactionSupport with Logging {
    override protected def flintClient: FlintClient = mockFlintClient
    override protected def dataSourceName: String = testDataSource
  }

  test("with transaction without force initialization") {
    transactionSupport.withTransaction[Unit](testIndex, testOpName) { _ => }

    verify(mockFlintClient, times(1)).startTransaction(testIndex, testDataSource, false)
  }

  test("with transaction with force initialization") {
    transactionSupport.withTransaction[Unit](testIndex, testOpName, forceInit = true) { _ => }

    verify(mockFlintClient, times(1)).startTransaction(testIndex, testDataSource, true)
  }

  test("should throw exception with nested exception message") {
    the[IllegalStateException] thrownBy {
      transactionSupport.withTransaction[Unit](testIndex, testOpName) { _ =>
        throw new IllegalArgumentException("fake exception")
      }
    } should have message
      "Failed to execute index operation [test operation] caused by: fake exception"
  }

  test("should throw exception with root cause exception message") {
    the[IllegalStateException] thrownBy {
      transactionSupport.withTransaction[Unit](testIndex, testOpName) { _ =>
        val rootCause = new IllegalArgumentException("fake root cause")
        val cause = new RuntimeException("message ignored", rootCause)
        throw new IllegalStateException("message ignored", cause)
      }
    } should have message
      "Failed to execute index operation [test operation] caused by: fake root cause"
  }
}
