/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.exception

/**
 * A throwable that exposes only an already-sanitized message while preserving the original
 * exception's type name and stack trace.
 *
 * Spark's `ExtendedAnalysisException` and `ParseException` embed customer query content in their
 * `getMessage` (the logical plan tree and the raw SQL text respectively). The caller computes a
 * sanitized message (e.g. via `AnalysisException.getSimpleMessage`, which Spark documents as
 * emitting the diagnostic without the plan) and wraps the original throwable in this class before
 * it reaches any logger or is forwarded downstream. That ensures the query content cannot leak
 * through `getMessage`, `getLocalizedMessage`, `toString`, or the rendered stack trace, while
 * still keeping the human-readable diagnostic and frames needed to debug where the error arose.
 *
 * @param originalClassName
 *   fully qualified name of the original exception type, surfaced in the rendered stack trace
 *   header so the error remains recognizable
 * @param redactedMessage
 *   the already-sanitized message (must not contain the plan or SQL text)
 */
class RedactedException(originalClassName: String, redactedMessage: String)
    extends RuntimeException(redactedMessage) {

  // log4j renders a throwable's stack trace header from toString(); override it so the header
  // reads "<original type>: <redacted message>" instead of exposing this wrapper's class name.
  override def toString: String = {
    val msg = getMessage
    if (msg == null) originalClassName else s"$originalClassName: $msg"
  }
}
