/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.util

import org.opensearch.flint.core.logging.{CustomLogging, OperationMessage}

import org.apache.spark.sql.ErrorSanitizer

/**
 * Handles and manages exceptions and error messages during each emr job run. Provides methods to
 * set, retrieve, and reset exception information.
 */
class ThrowableHandler {
  private var _throwableOption: Option[Throwable] = None
  private var _error: String = _

  def exceptionThrown: Option[Throwable] = _throwableOption
  def error: String = _error

  def recordThrowable(err: String, t: Throwable): Unit = {
    _error = err
    _throwableOption = Some(t)
    CustomLogging.logError(err, t)
  }

  /**
   * Records query-path failures with separate persisted and operator-safe messages. Query paths
   * must use this method; [[recordThrowable]] is reserved for infrastructure failures that cannot
   * carry query content. The original throwable remains available through [[exceptionThrown]].
   */
  def recordQueryThrowable(
      persistedError: String,
      operatorContext: String,
      t: Throwable): Unit = {
    _error = persistedError
    _throwableOption = Some(t)
    val (operationMessage, logSafeThrowable) = operatorLogEvent(operatorContext, t)
    CustomLogging.logError(operationMessage, logSafeThrowable)
  }

  /** Values handed to the logger, exposed for no-leak contract tests. */
  private[sql] def operatorLogRendering(
      operatorContext: String,
      t: Throwable): (String, Throwable) =
    (
      s"$operatorContext: ${ErrorSanitizer.operatorLogMessage(t)}",
      ErrorSanitizer.redactThrowable(t))

  /** Structured operator event with typed classification and correlation fields. */
  private[sql] def operatorLogEvent(
      operatorContext: String,
      t: Throwable): (OperationMessage, Throwable) = {
    val (message, logSafeThrowable) = operatorLogRendering(operatorContext, t)
    val classification = ErrorSanitizer.classify(t)
    val context = ErrorSanitizer.operatorLogContext(t)
    val operationMessage = new OperationMessage(
      message,
      classification.statusCode.map(Int.box).orNull,
      classification.errorCode,
      t.getClass.getName,
      context.requestId.orNull,
      context.extendedRequestId.orNull)
    (operationMessage, logSafeThrowable)
  }

  def setError(err: String): Unit = {
    _error = err
  }

  def setThrowable(t: Throwable): Unit = {
    _throwableOption = Some(t)
  }

  def reset(): Unit = {
    _throwableOption = None
    _error = null
  }

  def hasException: Boolean = _throwableOption.isDefined
}
