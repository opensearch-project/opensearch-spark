/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.storage

import java.util

import org.opensearch.flint.common.FlintVersion
import org.opensearch.flint.common.metadata.{FlintIndexMetadataService, FlintMetadata}
import org.opensearch.flint.core.FlintOptions
import org.opensearch.flint.core.metadata.FlintJsonHelper._

import org.apache.spark.internal.Logging

class FlintOpenSearchIndexMetadataService(options: FlintOptions)
    extends FlintIndexMetadataService
    with Logging {

  override def getIndexMetadata(indexName: String): FlintMetadata = {
    FlintMetadata(
      FlintVersion.current,
      "",
      "",
      "",
      Array(),
      util.Map.of(),
      util.Map.of(),
      util.Map.of(),
      None,
      None,
      None)
  }

  override def getAllIndexMetadata(indexNamePattern: String*): util.Map[String, FlintMetadata] = {
    util.Map.of()
  }

  override def updateIndexMetadata(indexName: String, metadata: FlintMetadata): Unit = {}
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
