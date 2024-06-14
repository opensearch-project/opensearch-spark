/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.metadata.log;

import java.util.Optional;

/**
 * Flint metadata log service provides API for metadata log related operations on a Flint index
 * regardless of underlying storage.
 */
public interface FlintMetadataLogService {

  /**
   * Start a new optimistic transaction.
   *
   * @param indexName index name
   * @param forceInit force init transaction and create empty metadata log if not exist
   * @return transaction handle
   */
  <T> OptimisticTransaction<T> startTransaction(String indexName, boolean forceInit);

  /**
   * Start a new optimistic transaction.
   *
   * @param indexName index name
   * @return transaction handle
   */
  default <T> OptimisticTransaction<T> startTransaction(String indexName) {
    return startTransaction(indexName, false);
  }

  /**
   * Get metadata log for index.
   *
   * @param indexName index name
   * @return optional metadata log
   */
  Optional<FlintMetadataLog<FlintMetadataLogEntry>> getIndexMetadataLog(String indexName);

  /**
   * Record heartbeat timestamp for index streaming job.
   *
   * @param indexName index name
   */
  void recordHeartbeat(String indexName);
}
