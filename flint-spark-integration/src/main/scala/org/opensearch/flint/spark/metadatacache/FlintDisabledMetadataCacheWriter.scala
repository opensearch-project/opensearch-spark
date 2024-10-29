/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.metadatacache

import org.opensearch.flint.common.metadata.FlintMetadata

/**
 * Default implementation of {@link FlintMetadataCacheWriter} that does nothing
 */
class FlintDisabledMetadataCacheWriter extends FlintMetadataCacheWriter {
  override def updateMetadataCache(indexName: String, metadata: FlintMetadata): Unit = {
    // Do nothing
  }
}
