/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql

import scala.concurrent.{ExecutionContextExecutor, Future}

import org.opensearch.flint.core.storage.FlintReader

case class InMemoryQueryExecutionState(
    recordedLastActivityTime: Long,
    recordedLastCanPickCheckTime: Long,
    recordedVerificationResult: VerificationResult,
    futurePrepareQueryExecution: Future[Either[String, Unit]],
    futureExecutor: ExecutionContextExecutor)
