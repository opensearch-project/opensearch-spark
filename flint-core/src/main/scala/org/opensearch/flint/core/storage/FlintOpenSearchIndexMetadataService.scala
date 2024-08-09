/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.storage

import java.util

import scala.collection.JavaConverters.mapAsJavaMapConverter

import org.opensearch.client.RequestOptions
import org.opensearch.client.indices.{GetIndexRequest, GetIndexResponse, PutMappingRequest}
import org.opensearch.common.xcontent.XContentType
import org.opensearch.flint.common.FlintVersion
import org.opensearch.flint.common.metadata.{FlintIndexMetadataService, FlintMetadata}
import org.opensearch.flint.core.FlintOptions
import org.opensearch.flint.core.metadata.FlintJsonHelper._

import org.apache.spark.internal.Logging

class FlintOpenSearchIndexMetadataService(options: FlintOptions)
    extends FlintIndexMetadataService
    with Logging {

  override def getIndexMetadata(indexName: String): FlintMetadata = {
    logInfo(s"Fetching Flint index metadata for $indexName")
    val osIndexName = OpenSearchClientUtils.sanitizeIndexName(indexName)
    val client = OpenSearchClientUtils.createClient(options)
    try {
      val request = new GetIndexRequest(osIndexName)
      val response = client.getIndex(request, RequestOptions.DEFAULT)
      val mapping = response.getMappings.get(osIndexName)
      val settings = response.getSettings.get(osIndexName)
      FlintOpenSearchIndexMetadataService.deserialize(mapping.source.string, settings.toString)
    } catch {
      case e: Exception =>
        throw new IllegalStateException(
          "Failed to get Flint index metadata for " + osIndexName,
          e)
    } finally {
      client.close()
    }
  }

  override def getAllIndexMetadata(indexNamePattern: String*): util.Map[String, FlintMetadata] = {
    logInfo(s"Fetching all Flint index metadata for pattern ${indexNamePattern.mkString(",")}");
    val indexNames = indexNamePattern.map(OpenSearchClientUtils.sanitizeIndexName)
    val client = OpenSearchClientUtils.createClient(options)
    try {
      val request = new GetIndexRequest(indexNames: _*)
      val response: GetIndexResponse = client.getIndex(request, RequestOptions.DEFAULT)

      response.getIndices
        .map(index =>
          index -> FlintOpenSearchIndexMetadataService.deserialize(
            response.getMappings.get(index).source().string(),
            response.getSettings.get(index).toString))
        .toMap
        .asJava
    } catch {
      case e: Exception =>
        throw new IllegalStateException(
          s"Failed to get Flint index metadata for ${indexNames.mkString(",")}",
          e)
    } finally {
      client.close()
    }
  }

  override def updateIndexMetadata(indexName: String, metadata: FlintMetadata): Unit = {
    logInfo(s"Updating Flint index $indexName with metadata $metadata");
    val osIndexName = OpenSearchClientUtils.sanitizeIndexName(indexName)
    val client = OpenSearchClientUtils.createClient(options)
    try {
      val request = new PutMappingRequest(osIndexName)
      request.source(FlintOpenSearchIndexMetadataService.serialize(metadata), XContentType.JSON)
      client.updateIndexMapping(request, RequestOptions.DEFAULT)
    } catch {
      case e: Exception =>
        throw new IllegalStateException(s"Failed to update Flint index $osIndexName", e)
    } finally {
      client.close()
    }
  }

  // Do nothing. For OpenSearch, deleting the index will also delete its metadata
  override def deleteIndexMetadata(indexName: String): Unit = {}
}

object FlintOpenSearchIndexMetadataService {

  /**
   * Generate JSON content as index metadata.
   *
   * @return
   *   JSON content
   */
  def serialize(metadata: FlintMetadata): String = {
    try {
      buildJson(builder => {
        // Add _meta field
        objectField(builder, "_meta") {
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
          optionalObjectField(builder, "properties", metadata.properties)
        }

        // Add properties (schema) field
        builder.field("properties", metadata.schema)
      })
    } catch {
      case e: Exception =>
        throw new IllegalStateException("Failed to jsonify Flint metadata", e)
    }
  }

  /**
   * Construct Flint metadata with JSON content and index settings.
   *
   * @param content
   *   JSON content
   * @param settings
   *   index settings
   * @return
   *   Flint metadata
   */
  def deserialize(content: String, settings: String): FlintMetadata = {
    val metadata = deserialize(content)
    metadata.copy(indexSettings = Option(settings))
  }

  /**
   * Parse the given JSON content and construct Flint metadata class.
   *
   * @param content
   *   JSON content
   * @return
   *   Flint metadata
   */
  def deserialize(content: String): FlintMetadata = {
    try {
      val builder = new FlintMetadata.Builder()
      parseJson(content) { (parser, fieldName) =>
        {
          fieldName match {
            case "_meta" =>
              parseObjectField(parser) { (parser, innerFieldName) =>
                {
                  innerFieldName match {
                    case "version" => builder.version(FlintVersion.apply(parser.text()))
                    case "name" => builder.name(parser.text())
                    case "kind" => builder.kind(parser.text())
                    case "source" => builder.source(parser.text())
                    case "indexedColumns" =>
                      parseArrayField(parser) {
                        builder.addIndexedColumn(parser.map())
                      }
                    case "options" => builder.options(parser.map())
                    case "properties" => builder.properties(parser.map())
                    case _ => // Handle other fields as needed
                  }
                }
              }
            case "properties" =>
              builder.schema(parser.map())
            case _ => // Ignore other fields, for instance, dynamic.
          }
        }
      }
      builder.build()
    } catch {
      case e: Exception =>
        throw new IllegalStateException("Failed to parse metadata JSON", e)
    }
  }
}
