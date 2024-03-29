/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

/**
 * Flint Spark exception.
 */
abstract class FlintSparkException(message: String, cause: Option[Throwable])
    extends Throwable(message) {}

object FlintSparkException {

  def requireValidation(requirement: Boolean, message: => Any): Unit = {
    if (!requirement) {
      throw new FlintSparkValidationException(message.toString)
    }
  }
}

/**
 * Flint Spark validation exception.
 *
 * @param message
 *   error message
 * @param cause
 *   exception causing the error
 */
class FlintSparkValidationException(message: String, cause: Option[Throwable] = None)
    extends FlintSparkException(message, cause)
