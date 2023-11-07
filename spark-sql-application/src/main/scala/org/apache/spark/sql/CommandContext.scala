/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql

import scala.concurrent.{ExecutionContextExecutor, Future}

import org.opensearch.flint.core.storage.{FlintReader, OpenSearchUpdater}

case class CommandContext(
    flintReader: FlintReader,
    spark: SparkSession,
    dataSource: String,
    resultIndex: String,
    sessionId: String,
    futureMappingCheck: Future[Either[String, Unit]],
    executionContext: ExecutionContextExecutor,
    flintSessionIndexUpdater: OpenSearchUpdater,
    osClient: OSClient,
    sessionIndex: String,
    jobId: String)
