/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql

import org.apache.spark.internal.Logging
import org.apache.spark.sql.flint.config.FlintSparkConf

/**
 * Temporary default implementation for QueryMetadataService. This should be replaced with an
 * implementation which write status to OpenSearch index
 */
class NoOpQueryMetadataService(flintSparkConf: FlintSparkConf)
    extends QueryMetadataService
    with Logging {

  override def updateQueryState(queryId: String, state: String, error: String): Unit =
    logInfo(s"updateQueryState: queryId=${queryId}, state=`${state}`, error=`${error}`")
}
