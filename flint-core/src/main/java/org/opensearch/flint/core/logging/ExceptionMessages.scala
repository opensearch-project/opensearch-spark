/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.logging

// Define constants for common error messages
object ExceptionMessages {
  val SyntaxErrorPrefix = "Syntax error"
  val S3ErrorPrefix = "Fail to read data from S3. Cause"
  val GlueErrorPrefix = "Fail to read data from Glue. Cause"
  val QueryAnalysisErrorPrefix = "Fail to analyze query. Cause"
  val SparkExceptionErrorPrefix = "Spark exception. Cause"
  val QueryRunErrorPrefix = "Fail to run query. Cause"
  val GlueAccessDeniedMessage = "Access denied in AWS Glue service. Please check permissions."
}
