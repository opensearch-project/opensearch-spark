/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql

/** Interface for updating query state and error. */
trait QueryMetadataService {
  def updateQueryState(queryId: String, state: String, error: String): Unit
}
