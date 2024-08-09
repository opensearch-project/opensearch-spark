/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.common.metadata;

import java.util.Map;

/**
 * Flint index metadata service provides API for index metadata related operations on a Flint index
 * regardless of underlying storage.
 * <p>
 * Custom implementations of this interface are expected to provide a public constructor with
 * the signature {@code public MyCustomService(SparkConf sparkConf)} to be instantiated by
 * the FlintIndexMetadataServiceBuilder.
 */
public interface FlintIndexMetadataService {

  /**
   * Retrieve metadata for a Flint index.
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
   * Update metadata for a Flint index.
   *
   * @param indexName index name
   * @param metadata index metadata to update
   */
  void updateIndexMetadata(String indexName, FlintMetadata metadata);

  /**
   * Delete metadata for a Flint index.
   *
   * @param indexName index name
   */
  void deleteIndexMetadata(String indexName);
}
