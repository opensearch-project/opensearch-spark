/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.http

import java.net.{ConnectException, SocketTimeoutException}
import java.util
import java.util.concurrent.{ExecutionException, Future}

import org.apache.http.HttpResponse
import org.apache.http.concurrent.FutureCallback
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient
import org.apache.http.nio.protocol.{HttpAsyncRequestProducer, HttpAsyncResponseConsumer}
import org.apache.http.protocol.HttpContext
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.mockito.verification.VerificationMode
import org.opensearch.flint.core.FlintOptions
import org.opensearch.flint.core.http.FlintRetryOptions.DEFAULT_MAX_RETRIES
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar.mock

class RetryableHttpAsyncClientSuite extends AnyFlatSpec with BeforeAndAfter with Matchers {

  /** Mocked internal client and future callback */
  val internalClient: CloseableHttpAsyncClient = mock[CloseableHttpAsyncClient]
  val future: Future[HttpResponse] = mock[Future[HttpResponse]]

  behavior of "Retryable HTTP async client"

  before {
    when(
      internalClient.execute(
        any[HttpAsyncRequestProducer],
        any[HttpAsyncResponseConsumer[HttpResponse]],
        any[HttpContext],
        any[FutureCallback[HttpResponse]])).thenReturn(future)
  }

  after {
    reset(internalClient, future)
  }

  it should "retry if exception is on the retryable exception list" in {
    Seq(new ConnectException).foreach { ex =>
      retryableClient
        .whenThrow(ex)
        .shouldExecute(times(DEFAULT_MAX_RETRIES + 1))
    }
  }

  it should "retry if exception's root cause is on the retryable exception list" in {
    retryableClient
      .whenThrow(new IllegalStateException(new ConnectException))
      .shouldExecute(times(DEFAULT_MAX_RETRIES + 1))
  }

  it should "not retry if exception is not on the retryable exception list" in {
    retryableClient
      .whenThrow(new SocketTimeoutException)
      .shouldExecute(times(1))
  }

  it should "retry with configured max attempt count" in {
    retryableClient
      .withOption("retry.max_retries", "1")
      .whenThrow(new ConnectException)
      .shouldExecute(times(2))
  }

  it should "retry if exception is configured in Flint options" in {
    Seq(new SocketTimeoutException).foreach { ex =>
      retryableClient
        .withOption("retry.exception_class_names", "java.net.SocketTimeoutException")
        .whenThrow(ex)
        .shouldExecute(times(DEFAULT_MAX_RETRIES + 1))
    }
  }

  private def retryableClient: AssertionHelper = new AssertionHelper

  class AssertionHelper {
    private val options: util.Map[String, String] = new util.HashMap[String, String]()

    def withOption(key: String, value: String): AssertionHelper = {
      options.put(key, value)
      this
    }

    def whenThrow(throwable: Throwable): AssertionHelper = {
      when(future.get()).thenThrow(new ExecutionException(throwable))
      this
    }

    def shouldExecute(expectTimes: VerificationMode): Unit = {
      val client =
        new RetryableHttpAsyncClient(internalClient, new FlintOptions(options))

      assertThrows[ExecutionException] {
        client.execute(null, null, null, null).get()
      }
      verify(internalClient, expectTimes)
        .execute(
          any[HttpAsyncRequestProducer],
          any[HttpAsyncResponseConsumer[HttpResponse]],
          any[HttpContext],
          any[FutureCallback[HttpResponse]])

      reset(future)
      clearInvocations(internalClient)
    }
  }
}
