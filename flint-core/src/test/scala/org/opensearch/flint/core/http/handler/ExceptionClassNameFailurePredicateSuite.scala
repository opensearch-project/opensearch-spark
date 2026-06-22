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

class ExceptionClassNameFailurePredicateSuite extends AnyFlatSpec with Matchers {

  behavior of "ExceptionClassNameFailurePredicate"

  it should "match by fully-qualified class name (backward compatible)" in {
    val predicate = new ExceptionClassNameFailurePredicate("java.net.ConnectException")
    predicate.test(new ConnectException("connect")) shouldBe true
    predicate.test(new SocketTimeoutException("read timed out")) shouldBe false
  }

  it should "match by simple class name" in {
    val predicate = new ExceptionClassNameFailurePredicate("ConnectException")
    predicate.test(new ConnectException("connect")) shouldBe true
  }

  it should "match a relocated/shaded exception by its simple name" in {
    // Different package, same simple name -> must match (shading-independent), which an
    // FQN/isInstance match would miss.
    val predicate = new ExceptionClassNameFailurePredicate("ConnectionClosedException")
    predicate.test(
      new _root_.shaded.org.apache.http.ConnectionClosedException(
        "Connection is closed")) shouldBe true
  }

  it should "match an exception nested in the cause chain" in {
    val predicate = new ExceptionClassNameFailurePredicate("ConnectionClosedException")
    val nested = new ExecutionException(
      new RuntimeException("wrapper", new ConnectionClosedException("Connection is closed")))
    predicate.test(nested) shouldBe true
  }

  it should "match any of the comma-separated names with mixed FQN and simple names" in {
    val predicate =
      new ExceptionClassNameFailurePredicate("java.net.ConnectException, NoHttpResponseException")
    predicate.test(new ConnectException("connect")) shouldBe true
    predicate.test(new NoHttpResponseException("failed to respond")) shouldBe true
  }

  it should "not match an exception that is not on the list" in {
    val predicate = new ExceptionClassNameFailurePredicate("java.net.ConnectException")
    Seq(
      new IllegalStateException("boom"),
      new IllegalArgumentException("bad"),
      new RuntimeException("generic")).foreach(ex => predicate.test(ex) shouldBe false)
  }

  it should "not match an unrelated IOException (narrow by exact name, not any IOException)" in {
    // A plain IOException with a non-listed simple name must NOT match, proving matching is by
    // exact class name rather than assignability to IOException.
    val predicate = new ExceptionClassNameFailurePredicate("ConnectionClosedException")
    predicate.test(new java.io.IOException("disk error")) shouldBe false
  }

  it should "ignore blank entries in the configured list" in {
    // Trailing/empty tokens (e.g. from a stray comma) must not turn into a match on everything.
    val predicate = new ExceptionClassNameFailurePredicate("ConnectException, ,")
    predicate.test(new ConnectException("connect")) shouldBe true
    predicate.test(new RuntimeException("generic")) shouldBe false
  }
}
