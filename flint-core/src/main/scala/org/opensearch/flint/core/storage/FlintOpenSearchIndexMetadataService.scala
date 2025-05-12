/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.storage

import java.util

import scala.collection.JavaConverters._

import org.opensearch.client.RequestOptions
import org.opensearch.client.indices.{GetIndexRequest, GetIndexResponse, PutMappingRequest}
import org.opensearch.common.xcontent.{XContentParser, XContentType}
import org.opensearch.flint.common.FlintVersion
import org.opensearch.flint.common.metadata.{FlintIndexMetadataService, FlintMetadata}
import org.opensearch.flint.core.FlintOptions
import org.opensearch.flint.core.IRestHighLevelClient
import org.opensearch.flint.core.metadata.FlintJsonHelper._

import org.apache.spark.internal.Logging

class FlintOpenSearchIndexMetadataService(options: FlintOptions)
    extends FlintIndexMetadataService
    with Logging {

  override def getIndexMetadata(indexName: String): FlintMetadata = {
    logInfo(s"Fetching Flint index metadata for $indexName")
    val osIndexName = OpenSearchClientUtils.sanitizeIndexName(indexName)
    var client: IRestHighLevelClient = null
    try {
      client = OpenSearchClientUtils.createClient(options)
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
    } finally
      if (client != null) {
        client.close()
      }
  }

  override def supportsGetByIndexPattern(): Boolean = true

  override def getAllIndexMetadata(
      indexNamePatterns: String*): util.Map[String, FlintMetadata] = {
    logInfo(s"Fetching all Flint index metadata for pattern ${indexNamePatterns.mkString(",")}");
    val indexNames = indexNamePatterns.map(OpenSearchClientUtils.sanitizeIndexName)
    var client: IRestHighLevelClient = null
    try {
      client = OpenSearchClientUtils.createClient(options)
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
    } finally
      if (client != null) {
        client.close()
      }
  }

  override def updateIndexMetadata(indexName: String, metadata: FlintMetadata): Unit = {
    logInfo(s"Updating Flint index $indexName with metadata $metadata");
    val osIndexName = OpenSearchClientUtils.sanitizeIndexName(indexName)
    var client: IRestHighLevelClient = null
    try {
      client = OpenSearchClientUtils.createClient(options)
      val request = new PutMappingRequest(osIndexName)
      request.source(FlintOpenSearchIndexMetadataService.serialize(metadata), XContentType.JSON)
      client.updateIndexMapping(request, RequestOptions.DEFAULT)
    } catch {
      case e: Exception =>
        throw new IllegalStateException(s"Failed to update Flint index $osIndexName", e)
    } finally
      if (client != null) {
        client.close()
      }
  }

  // Do nothing. For OpenSearch, deleting the index will also delete its metadata
  override def deleteIndexMetadata(indexName: String): Unit = {}
}

object FlintOpenSearchIndexMetadataService extends Logging {
  def serialize(metadata: FlintMetadata): String = {
    serialize(metadata, true)
  }

  /**
   * Generate JSON content as index metadata.
   *
   * @param metadata
   *   Flint index metadata
   * @param includeSpec
   *   Whether to include _meta field in the JSON content for Flint index specification
   * @return
   *   JSON content
   */
  def serialize(metadata: FlintMetadata, includeSpec: Boolean): String = {
    try {
      buildJson(builder => {
        if (includeSpec) {
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
        }
        // Add _source field
        val indexMappingsOpt =
          Option(metadata.options.get("index_mappings")).map(_.asInstanceOf[String])
        val sourceEnabled = extractSourceEnabled(indexMappingsOpt)
        if (!sourceEnabled) {
          objectField(builder, "_source") {
            builder.field("enabled", sourceEnabled)
          }
        }

        // Add properties (schema) field
        val schema = mergeSchema(metadata.schema, metadata.options)
        builder.field("properties", schema)
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
            case "_source" => parser.skipChildren()
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

  def extractSourceEnabled(indexMappingsJsonOpt: Option[String]): Boolean = {
    var sourceEnabled: Boolean = true

    indexMappingsJsonOpt.foreach { jsonStr =>
      try {
        parseJson(jsonStr) { (parser, fieldName) =>
          fieldName match {
            case "_source" =>
              parseObjectField(parser) { (parser, innerFieldName) =>
                innerFieldName match {
                  case "enabled" =>
                    sourceEnabled = parser.booleanValue()
                    return sourceEnabled
                  case _ => // Ignore
                }
              }
            case _ => // Ignore
          }
        }
      } catch {
        case e: Exception =>
          logError("Error extracting _source", e)
      }
    }

    sourceEnabled
  }

  /**
   * Merges the mapping parameters from FlintSparkIndexOptions into the existing schema. If the
   * options contain mapping parameters that exist in allFieldTypes, those configurations are
   * merged.
   *
   * @param allFieldTypes
   *   Map of field names to their type/configuration details
   * @param options
   *   FlintMetadata containing potential mapping parameters
   * @return
   *   Merged map with combined mapping parameters
   */
  def mergeSchema(
      allFieldTypes: java.util.Map[String, AnyRef],
      options: java.util.Map[String, AnyRef]): java.util.Map[String, AnyRef] = {
    val indexMappingsOpt = Option(options.get("index_mappings")).flatMap {
      case s: String => Some(s)
      case _ => None
    }

    var result = new java.util.HashMap[String, AnyRef](allFieldTypes)

    // Track mappings from leaf name to configuration properties
    var fieldConfigs = new java.util.HashMap[String, java.util.Map[String, AnyRef]]()

    indexMappingsOpt.foreach { jsonStr =>
      try {
        // Extract nested field configurations - key is the leaf name
        parseJson(jsonStr) { (parser, fieldName) =>
          fieldName match {
            case "_source" =>
              parser.skipChildren() // Skip _source section

            case "properties" =>
              // Process properties recursively to extract field configs
              fieldConfigs = extractNestedProperties(parser)

            case _ =>
              parser.skipChildren() // Skip other fields
          }
        }

        // Apply extracted configurations to schema while preserving structure
        val newResult = new java.util.HashMap[String, AnyRef]()
        result.forEach { (fullFieldName, fieldType) =>
          val leafFieldName = extractLeafFieldName(fullFieldName)

          if (fieldConfigs.containsKey(leafFieldName)) {
            // We have config for this leaf field name
            fieldType match {
              case existingConfig: java.util.Map[_, _] =>
                val mergedConfig = new java.util.HashMap[String, AnyRef](
                  existingConfig.asInstanceOf[java.util.Map[String, AnyRef]])

                // Add/overwrite with new config values
                fieldConfigs.get(leafFieldName).forEach { (k, v) =>
                  mergedConfig.put(k, v)
                }

                // Return the updated field with its original key
                newResult.put(fullFieldName, mergedConfig)

              case _ =>
                // If field type isn't a map, keep it unchanged
                newResult.put(fullFieldName, fieldType)
            }
          } else {
            // No config for this field, keep it unchanged
            newResult.put(fullFieldName, fieldType)
          }
        }
        result = newResult
      } catch {
        case ex: Exception =>
          logError("Error merging schema", ex)
      }
    }

    result
  }

  /**
   * Recursively extracts mapping parameters from nested properties structure. Returns a map of
   * field name to its configuration.
   */
  private def extractNestedProperties(
      parser: XContentParser): java.util.HashMap[String, java.util.Map[String, AnyRef]] = {

    val fieldConfigs = new java.util.HashMap[String, java.util.Map[String, AnyRef]]()

    parseObjectField(parser) { (parser, fieldName) =>
      val fieldConfig = new java.util.HashMap[String, AnyRef]()
      var hasNestedProperties = false

      parseObjectField(parser) { (parser, propName) =>
        if (propName == "properties") {
          hasNestedProperties = true
          val nestedConfigs = extractNestedProperties(parser)
          nestedConfigs.forEach { (k, v) =>
            fieldConfigs.put(k, v)
          }
        } else {
          val value = parser.currentToken() match {
            case XContentParser.Token.VALUE_STRING => parser.text()
            case XContentParser.Token.VALUE_NUMBER => parser.numberValue()
            case XContentParser.Token.VALUE_BOOLEAN => parser.booleanValue()
            case XContentParser.Token.VALUE_NULL => null
            case _ =>
              parser.skipChildren()
              null
          }

          if (value != null) {
            fieldConfig.put(propName, value.asInstanceOf[AnyRef])
          }
        }
      }

      if (!hasNestedProperties && !fieldConfig.isEmpty) {
        fieldConfigs.put(fieldName, fieldConfig)
      }
    }

    fieldConfigs
  }

  /**
   * Extracts the leaf field name from a potentially nested field path. For example:
   * "aws.vpc.count" -> "count"
   */
  private def extractLeafFieldName(fullFieldPath: String): String = {
    val lastDotIndex = fullFieldPath.lastIndexOf('.')
    if (lastDotIndex >= 0) {
      fullFieldPath.substring(lastDotIndex + 1)
    } else {
      fullFieldPath
    }
  }
}
