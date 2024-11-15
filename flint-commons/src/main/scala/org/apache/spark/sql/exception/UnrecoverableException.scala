/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.exception

/**
 * Represents an unrecoverable exception in session management and statement execution. This
 * exception is used for errors that cannot be handled or recovered from.
 */
class UnrecoverableException private (message: String, cause: Throwable)
    extends RuntimeException(message, cause) {

  def this(cause: Throwable) =
    this(cause.getMessage, cause)
}

object UnrecoverableException {
  def apply(cause: Throwable): UnrecoverableException =
    new UnrecoverableException(cause)

  def apply(message: String, cause: Throwable): UnrecoverableException =
    new UnrecoverableException(message, cause)
}
