/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.http

import java.net.{ConnectException, SocketTimeoutException}
import java.util
import java.util.Collections.emptyMap
import java.util.concurrent.{ExecutionException, Future}

import scala.collection.JavaConverters.mapAsJavaMapConverter

import org.apache.http.HttpEntity
import org.apache.http.HttpResponse
import org.apache.http.concurrent.FutureCallback
import org.apache.http.impl.nio.client.{CloseableHttpAsyncClient, HttpAsyncClientBuilder}
import org.apache.http.nio.protocol.{HttpAsyncRequestProducer, HttpAsyncResponseConsumer}
import org.apache.http.protocol.HttpContext
import org.apache.http.util.EntityUtils
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

  it should "return retry client builder by default" in {
    val builder = mock[HttpAsyncClientBuilder]
    val finalBuilder = RetryableHttpAsyncClient.builder(builder, new FlintOptions(emptyMap()))

    finalBuilder should not be builder
  }

  it should "return retry client builder if retry enabled (max_retries > 0)" in {
    val builder = mock[HttpAsyncClientBuilder]
    val finalBuilder = RetryableHttpAsyncClient.builder(
      builder,
      new FlintOptions(Map("retry.max_retries" -> "5").asJava))

    finalBuilder should not be builder
  }

  it should "return original client builder if retry disabled (max_retries = 0)" in {
    val builder = mock[HttpAsyncClientBuilder]
    val finalBuilder = RetryableHttpAsyncClient.builder(
      builder,
      new FlintOptions(Map("retry.max_retries" -> "0").asJava))

    finalBuilder shouldBe builder
  }

  it should "retry if response code is on the retryable status code list" in {
    Seq(429, 502).foreach { statusCode =>
      retryableClient
        .whenStatusCode(statusCode)
        .shouldExecute(times(DEFAULT_MAX_RETRIES + 1))
    }
  }

  it should "retry if response message contains retryable message" in {
    retryableClient
      .whenResponse(
        400,
        "OpenSearchStatusException[OpenSearch exception [type=resource_already_exists_exception,")
      .shouldExecute(times(DEFAULT_MAX_RETRIES + 1))
  }

  it should "not retry if response code is not on the retryable status code list" in {
    retryableClient
      .whenStatusCode(400)
      .shouldExecute(times(1))
  }

  it should "not retry any exception by default" in {
    retryableClient
      .whenThrow(new ConnectException)
      .shouldExecute(times(1))
  }

  it should "retry if exception is on the retryable exception list" in {
    Seq(new ConnectException, new SocketTimeoutException).foreach { ex =>
      retryableClient
        .withOption(
          "retry.exception_class_names",
          "java.net.ConnectException,java.net.SocketTimeoutException")
        .whenThrow(ex)
        .shouldExecute(times(DEFAULT_MAX_RETRIES + 1))
    }
  }

  it should "retry if exception's root cause is on the retryable exception list" in {
    retryableClient
      .withOption("retry.exception_class_names", "java.net.ConnectException")
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
      .whenStatusCode(429)
      .shouldExecute(times(2))
  }

  it should "return if retry successfully" in {
    val response = mock[HttpResponse](RETURNS_DEEP_STUBS)
    when(future.get()).thenReturn(response)
    when(response.getStatusLine.getStatusCode)
      .thenReturn(429)
      .thenReturn(429)
      .thenReturn(200)

    retryableClient
      .shouldExecute(times(3))
  }

  // Exception like AmazonServiceException is thrown from interceptor in execute() directly
  it should "retry too if exception thrown from execute instead of future get" in {
    reset(internalClient)
    when(
      internalClient.execute(
        any[HttpAsyncRequestProducer],
        any[HttpAsyncResponseConsumer[HttpResponse]],
        any[HttpContext],
        any[FutureCallback[HttpResponse]])).thenThrow(new IllegalStateException)

    retryableClient
      .withOption("retry.exception_class_names", "java.lang.IllegalStateException")
      .shouldExecute(
        expectExecuteTimes = times(DEFAULT_MAX_RETRIES + 1),
        expectFutureGetTimes = times(0))
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

    def whenStatusCode(statusCode: Int): AssertionHelper = {
      val response = mock[HttpResponse](RETURNS_DEEP_STUBS)
      when(response.getStatusLine.getStatusCode).thenReturn(statusCode)
      when(future.get()).thenReturn(response)
      this
    }

    def whenResponse(statusCode: Int, responseMessage: String): AssertionHelper = {
      val entity = mock[HttpEntity](RETURNS_DEEP_STUBS)
      mockStatic(classOf[EntityUtils])
      when(EntityUtils.toString(any[HttpEntity])).thenReturn(responseMessage)
      val response = mock[HttpResponse](RETURNS_DEEP_STUBS)
      when(response.getStatusLine.getStatusCode).thenReturn(statusCode)
      when(response.getEntity).thenReturn(entity)
      when(future.get()).thenReturn(response)
      this
    }

    def shouldExecute(expectExecuteTimes: VerificationMode): Unit = {
      shouldExecute(expectExecuteTimes, expectExecuteTimes)
    }

    def shouldExecute(
        expectExecuteTimes: VerificationMode,
        expectFutureGetTimes: VerificationMode): Unit = {
      val client =
        new RetryableHttpAsyncClient(internalClient, new FlintOptions(options).getRetryOptions)

      try {
        client.execute(null, null, null, null).get()
      } catch {
        case _: Throwable => // Ignore because we're testing error case
      } finally {
        // Verify `execute(...).get()` was called with expected times
        verify(internalClient, expectExecuteTimes)
          .execute(
            any[HttpAsyncRequestProducer],
            any[HttpAsyncResponseConsumer[HttpResponse]],
            any[HttpContext],
            any[FutureCallback[HttpResponse]])
        verify(future, expectFutureGetTimes).get()

        reset(future)
        clearInvocations(internalClient)
      }
    }
  }
}
