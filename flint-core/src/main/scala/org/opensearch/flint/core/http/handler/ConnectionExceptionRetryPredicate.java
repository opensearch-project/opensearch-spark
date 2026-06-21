/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.http.handler;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Failure predicate that retries transient connection-level faults, matched by the
 * exception's <em>simple class name</em> over the whole cause-chain.
 *
 * <p>Why simple name (not the fully-qualified class): the OpenSearch REST client relocates
 * (shades) Apache HTTP, so at runtime the thrown class is e.g. a relocated
 * {@code <shaded-prefix>.org.apache.http.ConnectionClosedException} -- a different class than the
 * unshaded {@code org.apache.http.ConnectionClosedException}. An {@code isInstance} / FQN match
 * (as in {@link ExceptionClassNameFailurePredicate}) would miss it, and loading the unshaded
 * name only to test {@code isInstance} would not match either. Matching the simple name
 * ({@code ConnectionClosedException}) is package/shading agnostic and avoids loading classes.
 *
 * <p>The set is intentionally narrow -- only connection-level faults where the request either
 * never reached the server or no complete response was received, which are safe to retry for
 * idempotent operations. This is deliberately NOT "any IOException", to avoid retrying
 * protocol/parse/SSL errors that happen to extend {@code IOException}.
 */
public class ConnectionExceptionRetryPredicate extends ErrorStacktraceFailurePredicate {

  /**
   * Simple class names of transient connection-level exceptions that are safe to retry.
   * Kept as simple names so matching is independent of shading/relocation.
   */
  static final Set<String> RETRYABLE_CONNECTION_EXCEPTION_SIMPLE_NAMES =
      Arrays.stream(new String[] {
              "ConnectException",            // java.net - connection could not be established
              "ConnectionClosedException",   // org.apache.http - connection closed mid-flight
              "NoHttpResponseException",     // org.apache.http - server closed before responding
              "ConnectionPoolTimeoutException", // org.apache.http - timed out leasing a connection
              "SocketTimeoutException"       // java.net - read/connect timed out
          })
          .collect(Collectors.toSet());

  @Override
  protected boolean isRetryable(Throwable throwable) {
    return RETRYABLE_CONNECTION_EXCEPTION_SIMPLE_NAMES.contains(
        throwable.getClass().getSimpleName());
  }
}
