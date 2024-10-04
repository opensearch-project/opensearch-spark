/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.storage

import java.util

import scala.collection.JavaConverters._

import org.opensearch.client.RequestOptions
import org.opensearch.client.indices.PutMappingRequest
import org.opensearch.common.xcontent.XContentType
import org.opensearch.flint.common.metadata.FlintMetadata
import org.opensearch.flint.core.{FlintOptions, IRestHighLevelClient}
import org.opensearch.flint.core.metadata.{FlintIndexMetadataServiceBuilder, FlintMetadataCache}
import org.opensearch.flint.core.metadata.FlintJsonHelper._

import org.apache.spark.internal.Logging

/**
 * Writes {@link FlintMetadataCache} to index mappings `_meta` for frontend user to access. This
 * is different from {@link FlintIndexMetadataService} which persists the full index metadata to a
 * storage for single source of truth.
 */
class FlintOpenSearchMetadataCacheWriter(options: FlintOptions) extends Logging {

  /**
   * Since metadata cache shares the index mappings _meta field with OpenSearch index metadata
   * storage, this flag is to allow for preserving index metadata that is already stored in _meta
   * when updating metadata cache.
   */
  private val includeSpec: Boolean =
    FlintIndexMetadataServiceBuilder
      .build(options)
      .isInstanceOf[FlintOpenSearchIndexMetadataService]

  /**
   * Update metadata cache for a Flint index.
   *
   * @param indexName
   *   index name
   * @param metadata
   *   index metadata to update the cache
   */
  def updateMetadataCache(indexName: String, metadata: FlintMetadata): Unit = {
    logInfo(s"Updating metadata cache for $indexName");
    val osIndexName = OpenSearchClientUtils.sanitizeIndexName(indexName)
    var client: IRestHighLevelClient = null
    try {
      client = OpenSearchClientUtils.createClient(options)
      val request = new PutMappingRequest(osIndexName)
      // TODO: make sure to preserve existing lastRefreshTime
      // Note that currently lastUpdateTime isn't used to construct FlintMetadataLogEntry
      request.source(serialize(metadata), XContentType.JSON)
      client.updateIndexMapping(request, RequestOptions.DEFAULT)
    } catch {
      case e: Exception =>
        throw new IllegalStateException(
          s"Failed to update metadata cache for Flint index $osIndexName",
          e)
    } finally
      if (client != null) {
        client.close()
      }
  }

  /**
   * Serialize FlintMetadataCache from FlintMetadata. Modified from {@link
   * FlintOpenSearchIndexMetadataService}
   */
  private def serialize(metadata: FlintMetadata): String = {
    try {
      buildJson(builder => {
        objectField(builder, "_meta") {
          // If _meta is used as index metadata storage, preserve them.
          if (includeSpec) {
            builder
              .field("version", metadata.version.version)
              .field("name", metadata.name)
              .field("kind", metadata.kind)
              .field("source", metadata.source)
              .field("indexedColumns", metadata.indexedColumns)

            if (metadata.latestId.isDefined) {
              builder.field("latestId", metadata.latestId.get)
            }
            optionalObjectField(builder, "options", metadata.options)
          }

          optionalObjectField(builder, "properties", buildPropertiesMap(metadata))
        }
        builder.field("properties", metadata.schema)
      })
    } catch {
      case e: Exception =>
        throw new IllegalStateException("Failed to jsonify cache metadata", e)
    }
  }

  /**
   * Since _meta.properties is shared by both index metadata and metadata cache, here we merge the
   * two maps.
   */
  private def buildPropertiesMap(metadata: FlintMetadata): util.Map[String, AnyRef] = {
    val metadataCacheProperties = FlintMetadataCache.mock.toMap

    if (includeSpec) {
      (metadataCacheProperties ++ metadata.properties.asScala).asJava
    } else {
      metadataCacheProperties.asJava
    }
  }
}
