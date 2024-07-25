/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core;

import java.util.Map;

import org.opensearch.flint.core.metadata.FlintMetadata;
import org.opensearch.flint.core.storage.FlintReader;
import org.opensearch.flint.core.storage.FlintWriter;

/**
 * Flint index client that provides API for metadata and data operations
 * on a Flint index regardless of concrete storage.
 */
public interface FlintClient {

  /**
   * Create a Flint index with the metadata given.
   *
   * @param indexName index name
   * @param metadata  index metadata
   */
  void createIndex(String indexName, FlintMetadata metadata);

  /**
   * Does Flint index with the given name exist
   *
   * @param indexName index name
   * @return true if the index exists, otherwise false
   */
  boolean exists(String indexName);

  /**
   * Retrieve all metadata for Flint index whose name matches the given pattern.
   *
   * @param indexNamePattern index name pattern
   * @return map where the keys are the matched index names, and the values are
   *         corresponding index metadata
   */
  Map<String, FlintMetadata> getAllIndexMetadata(String... indexNamePattern);

  /**
   * Retrieve metadata in a Flint index.
   *
   * @param indexName index name
   * @return index metadata
   */
  FlintMetadata getIndexMetadata(String indexName);

  /**
   * Update a Flint index with the metadata given.
   *
   * @param indexName index name
   * @param metadata  index metadata
   */
  void updateIndex(String indexName, FlintMetadata metadata);

  /**
   * Delete a Flint index.
   *
   * @param indexName index name
   */
  void deleteIndex(String indexName);

  /**
   * Create {@link FlintWriter}.
   *
   * @param indexName - index name
   * @return {@link FlintWriter}
   */
  FlintWriter createWriter(String indexName);

  /**
   * Create {@link IRestHighLevelClient}.
   * @return {@link IRestHighLevelClient}
   */
  IRestHighLevelClient createClient();
}
