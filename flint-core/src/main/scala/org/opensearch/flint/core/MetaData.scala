/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core

import org.opensearch.flint.common.metadata.FlintMetadata
import org.opensearch.flint.core.storage.FlintOpenSearchIndexMetadataService

/**
 * OpenSearch Table metadata.
 *
 * @param name
 *   name
 * @param properties
 *   properties
 * @param setting
 *   setting
 */
case class MetaData(name: String, properties: String, setting: String)

object MetaData {
  def apply(name: String, flintMetadata: FlintMetadata): MetaData = {
    val properties = FlintOpenSearchIndexMetadataService.serialize(flintMetadata)
    val setting = flintMetadata.indexSettings.getOrElse("")
    MetaData(name, properties, setting)
  }
}
