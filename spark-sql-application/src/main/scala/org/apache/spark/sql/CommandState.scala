/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql

case class CommandState(
    recordedLastActivityTime: Long,
    recordedVerificationResult: VerificationResult)
