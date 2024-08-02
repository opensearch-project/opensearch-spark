/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.common.metadata;

import java.util.Map;

/**
 * TODO: comment
 */
public interface FlintIndexMetadataService {

  // TODO: createIndexMetadata?

  /**
   * Retrieve metadata in a Flint index.
   *
   * @param indexName index name
   * @return index metadata
   */
  FlintMetadata getIndexMetadata(String indexName);

  /**
   * Retrieve all metadata for Flint index whose name matches the given pattern.
   *
   * @param indexNamePattern index name pattern
   * @return map where the keys are the matched index names, and the values are
   *         corresponding index metadata
   */
  Map<String, FlintMetadata> getAllIndexMetadata(String... indexNamePattern);

  /**
   * Update metadata in a Flint index.
   *
   * @param indexName index name
   * @param metadata index metadata to update
   */
  void updateIndexMetadata(String indexName, FlintMetadata metadata);
}
