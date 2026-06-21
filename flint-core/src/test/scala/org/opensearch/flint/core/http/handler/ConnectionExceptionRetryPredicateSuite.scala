/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.http.handler

import java.net.{ConnectException, SocketTimeoutException}
import java.util.concurrent.ExecutionException

import org.apache.http.{ConnectionClosedException, NoHttpResponseException}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ConnectionExceptionRetryPredicateSuite extends AnyFlatSpec with Matchers {

  private val predicate = new ConnectionExceptionRetryPredicate()

  behavior of "ConnectionExceptionRetryPredicate"

  it should "retry transient connection-level exceptions" in {
    Seq(
      new ConnectException("connect"),
      new SocketTimeoutException("read timed out"),
      new ConnectionClosedException("Connection is closed"),
      new NoHttpResponseException("failed to respond"))
      .foreach(ex => predicate.test(ex) shouldBe true)
  }

  it should "retry when the connection exception is nested in the cause chain" in {
    val nested = new ExecutionException(
      new RuntimeException("wrapper", new ConnectionClosedException("Connection is closed")))
    predicate.test(nested) shouldBe true
  }

  it should "retry a relocated/shaded connection exception by simple name" in {
    // Different package, same simple name -> must match (shading-independent).
    predicate.test(
      new _root_.shaded.org.apache.http.ConnectionClosedException("Connection is closed")) shouldBe true
  }

  it should "not retry non-connection exceptions" in {
    Seq(
      new IllegalStateException("boom"),
      new IllegalArgumentException("bad"),
      new RuntimeException("generic"))
      .foreach(ex => predicate.test(ex) shouldBe false)
  }

  it should "not retry an unrelated IOException (narrow by design, not any IOException)" in {
    // A plain IOException with a non-connection simple name must NOT be retried, proving the
    // predicate is narrower than "any IOException".
    predicate.test(new java.io.IOException("disk error")) shouldBe false
  }
}
