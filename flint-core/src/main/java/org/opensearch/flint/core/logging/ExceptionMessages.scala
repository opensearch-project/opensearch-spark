/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.logging

import com.amazonaws.services.s3.model.AmazonS3Exception

// Define constants for common error messages
object ExceptionMessages {
  val SyntaxErrorPrefix = "Syntax error"
  val S3ErrorPrefix = "Fail to read data from S3. Cause"
  val GlueErrorPrefix = "Fail to read data from Glue. Cause"
  val QueryAnalysisErrorPrefix = "Fail to analyze query. Cause"
  val SparkExceptionErrorPrefix = "Spark exception. Cause"
  val QueryRunErrorPrefix = "Fail to run query. Cause"
  val GlueAccessDeniedMessage = "Access denied in AWS Glue service. Please check permissions."

  /**
   * Extracts the root cause from a throwable and returns a sanitized error message.
   *
   * @param e
   *   The throwable from which to extract the root cause.
   * @param maxLength
   *   The maximum length of the returned error message. Default is 100 characters.
   * @return
   *   A sanitized and possibly truncated error message string.
   */
  def extractRootCause(e: Throwable, maxLength: Int = 1000): String = {
    truncateMessage(redactMessage(rootCause(e)), maxLength)
  }

  private def rootCause(e: Throwable): Throwable = {
    var cause = e
    while (cause.getCause != null && cause.getCause != cause) {
      cause = cause.getCause
    }
    cause
  }

  private def redactMessage(cause: Throwable): String = cause match {
    case e: AmazonS3Exception =>
      s"$S3ErrorPrefix: serviceName=[${e.getServiceName}], statusCode=[${e.getStatusCode}]"
    case _ =>
      if (cause.getLocalizedMessage != null) {
        cause.getLocalizedMessage
      } else if (cause.getMessage != null) {
        cause.getMessage
      } else {
        cause.toString
      }
  }

  private def truncateMessage(message: String, maxLength: Int): String = {
    if (message.length > maxLength) {
      message.take(maxLength) + "..."
    } else {
      message
    }
  }
}
