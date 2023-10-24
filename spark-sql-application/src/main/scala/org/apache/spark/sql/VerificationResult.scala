/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql

sealed trait VerificationResult

case object NotVerified extends VerificationResult
case object VerifiedWithoutError extends VerificationResult
case class VerifiedWithError(error: String) extends VerificationResult
