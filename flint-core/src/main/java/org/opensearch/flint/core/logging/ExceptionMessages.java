/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.logging;

import com.amazonaws.services.s3.model.AmazonS3Exception;

/**
 * Define constants for common error messages and utility methods to handle them.
 */
public class ExceptionMessages {
  public static final String SyntaxErrorPrefix = "Syntax error";
  public static final String S3ErrorPrefix = "Fail to read data from S3. Cause";
  public static final String GlueErrorPrefix = "Fail to read data from Glue. Cause";
  public static final String QueryAnalysisErrorPrefix = "Fail to analyze query. Cause";
  public static final String SparkExceptionErrorPrefix = "Spark exception. Cause";
  public static final String QueryRunErrorPrefix = "Fail to run query. Cause";
  public static final String GlueAccessDeniedMessage = "Access denied in AWS Glue service. Please check permissions.";

  /**
   * Extracts the root cause from a throwable and returns a sanitized error message.
   *
   * @param e         The throwable from which to extract the root cause.
   * @param maxLength The maximum length of the returned error message. Default is 1000 characters.
   * @return A sanitized and possibly truncated error message string.
   */
  public static String extractRootCause(Throwable e, int maxLength) {
    return truncateMessage(redactMessage(rootCause(e)), maxLength);
  }

  /**
   * Overloads extractRootCause to provide a non-truncated error message.
   *
   * @param e The throwable from which to extract the root cause.
   * @return A sanitized error message string.
   */
  public static String extractRootCause(Throwable e) {
    return redactMessage(rootCause(e));
  }

  private static Throwable rootCause(Throwable e) {
    Throwable cause = e;
    while (cause.getCause() != null && cause.getCause() != cause) {
      cause = cause.getCause();
    }
    return cause;
  }

  private static String redactMessage(Throwable cause) {
    if (cause instanceof AmazonS3Exception) {
      AmazonS3Exception e = (AmazonS3Exception) cause;
      return String.format("%s: serviceName=[%s], statusCode=[%d]",
          S3ErrorPrefix, e.getServiceName(), e.getStatusCode());
    } else {
      if (cause.getLocalizedMessage() != null) {
        return cause.getLocalizedMessage();
      } else if (cause.getMessage() != null) {
        return cause.getMessage();
      } else {
        return cause.toString();
      }
    }
  }

  private static String truncateMessage(String message, int maxLength) {
    if (message.length() > maxLength) {
      return message.substring(0, maxLength) + "...";
    } else {
      return message;
    }
  }
}
