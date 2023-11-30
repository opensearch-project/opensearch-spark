/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql

import scala.concurrent.{ExecutionContextExecutor, Future}

import org.opensearch.flint.core.storage.FlintReader

case class CommandState(
    recordedLastActivityTime: Long,
    recordedVerificationResult: VerificationResult,
    flintReader: FlintReader,
    futureMappingCheck: Future[Either[String, Unit]],
    executionContext: ExecutionContextExecutor,
    recordedLastCanPickCheckTime: Long)
