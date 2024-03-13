/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.metadata

import java.util

import org.opensearch.flint.core.FlintVersion
import org.opensearch.flint.core.FlintVersion.current
import org.opensearch.flint.core.metadata.FlintJsonHelper._
import org.opensearch.flint.core.metadata.log.FlintMetadataLogEntry

/**
 * Flint metadata follows Flint index specification and defines metadata for a Flint index
 * regardless of query engine integration and storage.
 */
case class FlintMetadata(
    /** Flint spec version */
    version: FlintVersion,
    /** Flint index name */
    name: String,
    /** Flint index kind */
    kind: String,
    /** Flint index source that index data derived from */
    source: String,
    /** Flint indexed column list */
    indexedColumns: Array[util.Map[String, AnyRef]] = Array(),
    /** Flint indexed options. TODO: move to properties? */
    options: util.Map[String, AnyRef] = new util.HashMap[String, AnyRef],
    /** Flint index properties for any custom fields */
    properties: util.Map[String, AnyRef] = new util.HashMap[String, AnyRef],
    /** Flint index schema */
    schema: util.Map[String, AnyRef] = new util.HashMap[String, AnyRef],
    /** Optional latest metadata log entry id */
    latestId: Option[String] = None,
    /** Optional latest metadata log entry */
    latestLogEntry: Option[FlintMetadataLogEntry] = None,
    /** Optional Flint index settings. TODO: move elsewhere? */
    indexSettings: Option[String]) {

  require(version != null, "version is required")
  require(name != null, "name is required")
  require(kind != null, "kind is required")
  require(source != null, "source is required")

  /**
   * Generate JSON content as index metadata.
   *
   * @return
   *   JSON content
   */
  def getContent: String = {
    try {
      buildJson(builder => {
        // Add _meta field
        objectField(builder, "_meta") {
          builder
            .field("version", version.version)
            .field("name", name)
            .field("kind", kind)
            .field("source", source)
            .field("indexedColumns", indexedColumns)

          if (latestId.isDefined) {
            builder.field("latestId", latestId.get)
          }
          optionalObjectField(builder, "options", options)
          optionalObjectField(builder, "properties", properties)
        }

        // Add properties (schema) field
        builder.field("properties", schema)
      })
    } catch {
      case e: Exception =>
        throw new IllegalStateException("Failed to jsonify Flint metadata", e)
    }
  }
}

object FlintMetadata {

  /**
   * Construct Flint metadata with JSON content, index settings, and latest log entry.
   *
   * @param content
   *   JSON content
   * @param settings
   *   index settings
   * @param latestLogEntry
   *   latest metadata log entry
   * @return
   *   Flint metadata
   */
  def apply(
      content: String,
      settings: String,
      latestLogEntry: FlintMetadataLogEntry): FlintMetadata = {
    val metadata = FlintMetadata(content, settings)
    metadata.copy(latestLogEntry = Option(latestLogEntry))
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
  def apply(content: String, settings: String): FlintMetadata = {
    val metadata = FlintMetadata(content)
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
  def apply(content: String): FlintMetadata = {
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

  def builder(): FlintMetadata.Builder = new Builder

  /**
   * Flint index metadata builder that can be extended by subclass to provide more custom build
   * method.
   */
  class Builder {
    private var version: FlintVersion = FlintVersion.current()
    private var name: String = ""
    private var kind: String = ""
    private var source: String = ""
    private var options: util.Map[String, AnyRef] = new util.HashMap[String, AnyRef]()
    private var indexedColumns: Array[util.Map[String, AnyRef]] = Array()
    private var properties: util.Map[String, AnyRef] = new util.HashMap[String, AnyRef]()
    private var schema: util.Map[String, AnyRef] = new util.HashMap[String, AnyRef]()
    private var latestId: Option[String] = None
    private var latestLogEntry: Option[FlintMetadataLogEntry] = None
    private var indexSettings: Option[String] = None

    def version(version: FlintVersion): this.type = {
      this.version = version
      this
    }

    def name(name: String): this.type = {
      this.name = name
      this
    }

    def kind(kind: String): this.type = {
      this.kind = kind
      this
    }

    def source(source: String): this.type = {
      this.source = source
      this
    }

    def options(options: util.Map[String, AnyRef]): this.type = {
      this.options = options
      this
    }

    def indexedColumns(indexedColumns: Array[util.Map[String, AnyRef]]): this.type = {
      this.indexedColumns = indexedColumns
      this
    }

    def addIndexedColumn(indexCol: util.Map[String, AnyRef]): this.type = {
      indexedColumns = indexedColumns :+ indexCol
      this
    }

    def properties(properties: util.Map[String, AnyRef]): this.type = {
      this.properties = properties
      this
    }

    def addProperty(key: String, value: AnyRef): this.type = {
      properties.put(key, value)
      this
    }

    def schema(schema: util.Map[String, AnyRef]): this.type = {
      this.schema = schema
      this
    }

    def schema(schema: String): this.type = {
      parseJson(schema) { (parser, fieldName) =>
        fieldName match {
          case "properties" => this.schema = parser.map()
          case _ => // do nothing
        }
      }
      this
    }

    def latestLogEntry(entry: FlintMetadataLogEntry): this.type = {
      this.latestId = Option(entry.id)
      this.latestLogEntry = Option(entry)
      this
    }

    def indexSettings(indexSettings: String): this.type = {
      this.indexSettings = Option(indexSettings)
      this
    }

    // Build method to create the FlintMetadata instance
    def build(): FlintMetadata = {
      FlintMetadata(
        if (version == null) current() else version,
        name = name,
        kind = kind,
        source = source,
        indexedColumns = indexedColumns,
        options = options,
        properties = properties,
        schema = schema,
        indexSettings = indexSettings,
        latestId = latestId,
        latestLogEntry = latestLogEntry)
    }
  }
}
