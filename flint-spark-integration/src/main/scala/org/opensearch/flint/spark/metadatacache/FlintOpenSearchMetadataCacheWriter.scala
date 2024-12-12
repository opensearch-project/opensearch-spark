/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.metadatacache

import java.util.{HashMap, Map => JMap}

import scala.collection.JavaConverters._

import org.opensearch.client.RequestOptions
import org.opensearch.client.indices.GetIndexRequest
import org.opensearch.client.indices.PutMappingRequest
import org.opensearch.common.xcontent.XContentType
import org.opensearch.flint.common.metadata.FlintMetadata
import org.opensearch.flint.core.{FlintOptions, IRestHighLevelClient}
import org.opensearch.flint.core.metadata.FlintJsonHelper._
import org.opensearch.flint.core.storage.OpenSearchClientUtils

import org.apache.spark.internal.Logging

/**
 * Writes {@link FlintMetadataCache} to index mappings `_meta` field for frontend user to access.
 */
class FlintOpenSearchMetadataCacheWriter(options: FlintOptions)
    extends FlintMetadataCacheWriter
    with Logging {

  override def updateMetadataCache(indexName: String, metadata: FlintMetadata): Unit = {
    logInfo(s"Updating metadata cache for $indexName");
    val osIndexName = OpenSearchClientUtils.sanitizeIndexName(indexName)
    var client: IRestHighLevelClient = null
    try {
      client = OpenSearchClientUtils.createClient(options)
      val existingMapping = getIndexMapping(client, osIndexName)
      val metadataCacheProperties = FlintMetadataCache(metadata).toMap.asJava
      val mergedMapping = mergeMapping(existingMapping, metadataCacheProperties)
      val serialized = buildJson(builder => {
        builder.field("_meta", mergedMapping.get("_meta"))
        builder.field("properties", mergedMapping.get("properties"))
      })
      updateIndexMapping(client, osIndexName, serialized)
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

  private[metadatacache] def getIndexMapping(
      client: IRestHighLevelClient,
      osIndexName: String): JMap[String, AnyRef] = {
    val request = new GetIndexRequest(osIndexName)
    val response = client.getIndex(request, RequestOptions.DEFAULT)
    response.getMappings.get(osIndexName).sourceAsMap()
  }

  private def mergeMapping(
      existingMapping: JMap[String, AnyRef],
      metadataCacheProperties: JMap[String, AnyRef]): JMap[String, AnyRef] = {
    val meta =
      existingMapping.getOrDefault("_meta", Map.empty.asJava).asInstanceOf[JMap[String, AnyRef]]
    val properties =
      meta.getOrDefault("properties", Map.empty.asJava).asInstanceOf[JMap[String, AnyRef]]
    val updatedProperties = new HashMap[String, AnyRef](properties)
    updatedProperties.putAll(metadataCacheProperties)
    val updatedMeta = new HashMap[String, AnyRef](meta)
    updatedMeta.put("properties", updatedProperties)
    val result = new HashMap[String, AnyRef](existingMapping)
    result.put("_meta", updatedMeta)
    result
  }

  private[metadatacache] def updateIndexMapping(
      client: IRestHighLevelClient,
      osIndexName: String,
      mappingSource: String): Unit = {
    val request = new PutMappingRequest(osIndexName)
    request.source(mappingSource, XContentType.JSON)
    client.updateIndexMapping(request, RequestOptions.DEFAULT)
  }
}
