/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.common.metadata.log;

import java.util.Optional;
import org.apache.spark.SparkConf;

/**
 * Flint metadata log service provides API for metadata log related operations on a Flint index
 * regardless of underlying storage.
 */
public abstract class FlintMetadataLogService {

  protected final SparkConf sparkConf;

  /**
   * Constructor.
   *
   * @param sparkConf spark configuration
   */
  public FlintMetadataLogService(SparkConf sparkConf) {
    this.sparkConf = sparkConf;
  }

  /**
   * Default constructor.
   */
  public FlintMetadataLogService() {
    this(null);
  }

  /**
   * Start a new optimistic transaction.
   *
   * @param indexName index name
   * @param forceInit force init transaction and create empty metadata log if not exist
   * @return transaction handle
   */
  public abstract <T> OptimisticTransaction<T> startTransaction(String indexName, boolean forceInit);

  /**
   * Start a new optimistic transaction.
   *
   * @param indexName index name
   * @return transaction handle
   */
  public <T> OptimisticTransaction<T> startTransaction(String indexName) {
    return startTransaction(indexName, false);
  }

  /**
   * Get metadata log for index.
   *
   * @param indexName index name
   * @return optional metadata log
   */
  public abstract Optional<FlintMetadataLog<FlintMetadataLogEntry>> getIndexMetadataLog(String indexName);

  /**
   * Record heartbeat timestamp for index streaming job.
   *
   * @param indexName index name
   */
  public abstract void recordHeartbeat(String indexName);
}
