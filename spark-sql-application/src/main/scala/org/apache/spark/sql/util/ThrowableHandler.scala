/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.util

import org.opensearch.flint.core.logging.CustomLogging

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
