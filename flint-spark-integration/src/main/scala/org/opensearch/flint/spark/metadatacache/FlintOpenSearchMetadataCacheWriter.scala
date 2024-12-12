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
      val indexMapping = getIndexMapping(client, osIndexName)
      val metadataCacheProperties = FlintMetadataCache(metadata).toMap.asJava
      mergeMetadataCacheProperties(indexMapping, metadataCacheProperties)
      val serialized = buildJson(builder => {
        builder.field("_meta", indexMapping.get("_meta"))
        builder.field("properties", indexMapping.get("properties"))
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

  /**
   * Merge metadata cache properties into index mapping in place. Metadata cache is written into
   * _meta.properties field of index mapping.
   */
  private def mergeMetadataCacheProperties(
      indexMapping: JMap[String, AnyRef],
      metadataCacheProperties: JMap[String, AnyRef]): Unit = {
    indexMapping
      .computeIfAbsent("_meta", _ => new HashMap[String, AnyRef]())
      .asInstanceOf[JMap[String, AnyRef]]
      .computeIfAbsent("properties", _ => new HashMap[String, AnyRef]())
      .asInstanceOf[JMap[String, AnyRef]]
      .putAll(metadataCacheProperties)
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
