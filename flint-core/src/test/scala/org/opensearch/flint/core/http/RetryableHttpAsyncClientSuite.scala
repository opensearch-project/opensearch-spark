/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.http

import java.net.{ConnectException, SocketTimeoutException}
import java.util.Collections.emptyMap
import java.util.concurrent.{ExecutionException, Future}

import org.apache.http.HttpResponse
import org.apache.http.concurrent.FutureCallback
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient
import org.apache.http.nio.protocol.{HttpAsyncRequestProducer, HttpAsyncResponseConsumer}
import org.apache.http.protocol.HttpContext
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{reset, times, verify, when}
import org.opensearch.flint.core.FlintOptions
import org.opensearch.flint.core.http.FlintRetryOptions.DEFAULT_MAX_ATTEMPT
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar.mock

class RetryableHttpAsyncClientSuite extends AnyFlatSpec with BeforeAndAfter with Matchers {

  /** Mocked internal client and future callback */
  val internalClient: CloseableHttpAsyncClient = mock[CloseableHttpAsyncClient]
  val future: Future[HttpResponse] = mock[Future[HttpResponse]]

  /** Retryable client being tested */
  // var retryableClient: CloseableHttpAsyncClient = _

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
    val client =
      new RetryableHttpAsyncClient(internalClient, new FlintOptions(emptyMap()))

    Seq(new ConnectException).foreach { ex =>
      when(future.get()).thenThrow(new ExecutionException(ex))

      assertThrows[ExecutionException] {
        client.execute(null, null, null, null).get()
      }
      verify(internalClient, times(DEFAULT_MAX_ATTEMPT + 1))
        .execute(
          any[HttpAsyncRequestProducer],
          any[HttpAsyncResponseConsumer[HttpResponse]],
          any[HttpContext],
          any[FutureCallback[HttpResponse]])
    }
  }

  it should "retry if exception's root cause is on the retryable exception list" in {
    val client =
      new RetryableHttpAsyncClient(internalClient, new FlintOptions(emptyMap()))
    when(future.get())
      .thenThrow(new ExecutionException(new IllegalStateException(new ConnectException)))

    assertThrows[ExecutionException] {
      client.execute(null, null, null, null).get()
    }
    verify(internalClient, times(DEFAULT_MAX_ATTEMPT + 1))
      .execute(
        any[HttpAsyncRequestProducer],
        any[HttpAsyncResponseConsumer[HttpResponse]],
        any[HttpContext],
        any[FutureCallback[HttpResponse]])
  }

  it should "not retry if exception is not on the retryable exception list" in {
    val client =
      new RetryableHttpAsyncClient(internalClient, new FlintOptions(emptyMap()))
    when(future.get()).thenThrow(new ExecutionException(new SocketTimeoutException))

    assertThrows[ExecutionException] {
      client.execute(null, null, null, null).get()
    }
    verify(internalClient, times(1))
      .execute(
        any[HttpAsyncRequestProducer],
        any[HttpAsyncResponseConsumer[HttpResponse]],
        any[HttpContext],
        any[FutureCallback[HttpResponse]])
  }
}
