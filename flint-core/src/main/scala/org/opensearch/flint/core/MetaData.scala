/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.table

import org.opensearch.flint.core.metadata.FlintMetadata

case class MetaData(name: String, properties: String, setting: String)

object MetaData {
  def apply(name: String, flintMetadata: FlintMetadata): MetaData = {
    val properties = flintMetadata.getContent
    val setting = flintMetadata.indexSettings.getOrElse("")
    MetaData(name, properties, setting)
  }
}
