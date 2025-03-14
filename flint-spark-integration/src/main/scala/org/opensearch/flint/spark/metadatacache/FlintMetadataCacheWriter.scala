/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.metadatacache

import org.opensearch.flint.common.metadata.{FlintIndexMetadataService, FlintMetadata}

/**
 * Writes {@link ExportedFlintMetadata} to a storage of choice. This is different from {@link
 * FlintIndexMetadataService} which persists the full index metadata to a storage for single
 * source of truth.
 */
trait FlintMetadataCacheWriter {

  /**
   * Update metadata cache for a Flint index.
   *
   * @param indexName
   *   index name
   * @param metadata
   *   index metadata to update the cache
   */
  def updateMetadataCache(indexName: String, metadata: FlintMetadata): Unit

}
