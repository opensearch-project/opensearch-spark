/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.metadata.log;

import java.util.Optional;

/**
 * Flint metadata log service provides API for metadata log related operations on a Flint index
 * regardless of concrete storage.
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
   * @param initIfNotExist create empty metadata log if not exist
   * @return optional metadata log
   */
  Optional<FlintMetadataLog<FlintMetadataLogEntry>> getIndexMetadataLog(String indexName, boolean initIfNotExist);

  /**
   * Get metadata log for index.
   *
   * @param indexName index name
   * @return optional metadata log
   */
  default Optional<FlintMetadataLog<FlintMetadataLogEntry>> getIndexMetadataLog(String indexName) {
    return getIndexMetadataLog(indexName, false);
  }
}
