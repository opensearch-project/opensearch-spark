/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.metadata

import java.util

import org.opensearch.flint.core.FlintVersion
import org.opensearch.flint.core.metadata.XContentBuilderHelper._

/**
 * Flint metadata follows Flint index specification and defines metadata for a Flint index
 * regardless of query engine integration and storage.
 */
case class FlintMetadata(
    version: FlintVersion = FlintVersion.current(),
    name: String,
    kind: String,
    source: String,
    indexedColumns: Array[util.Map[String, AnyRef]] = Array(),
    options: util.Map[String, AnyRef] = new util.HashMap[String, AnyRef],
    properties: util.Map[String, AnyRef] = new util.HashMap[String, AnyRef],
    schema: util.Map[String, AnyRef] = new util.HashMap[String, AnyRef],
    indexSettings: String = null) {

  def getContent: String = {
    try {
      buildJson(builder => {
        // Add _meta field
        objectField(
          builder,
          "_meta", {
            builder.field("version", versionOrDefault())
            builder.field("name", name)
            builder.field("kind", kind)
            builder.field("source", source)
            builder.field("indexedColumns", indexedColumns)

            optionalObjectField(builder, "options", options)
            optionalObjectField(builder, "properties", properties)
          })

        // Add properties (schema) field
        builder.field("properties", schema)
      })
    } catch {
      case e: Exception =>
        throw new IllegalStateException("Failed to jsonify Flint metadata", e)
    }
  }

  private def versionOrDefault(): String = {
    if (version == null) {
      FlintVersion.current().version
    } else {
      version.version
    }
  }
}

object FlintMetadata {

  def fromJson(content: String, settings: String): FlintMetadata = {
    val builder = new FlintMetadata.Builder()
    try {
      parseJson(
        content,
        (parser, fieldName) =>
          fieldName match {
            case "_meta" =>
              parseObjectField(
                parser,
                (parser, innerFieldName) =>
                  innerFieldName match {
                    case "version" => builder.version(FlintVersion.apply(parser.text()))
                    case "name" => builder.name(parser.text())
                    case "kind" => builder.kind(parser.text())
                    case "source" => builder.source(parser.text())
                    case "indexedColumns" =>
                      parseArrayField(
                        parser, {
                          builder.addIndexedColumn(parser.map())
                        })
                    case "options" => builder.options(parser.map())
                    case "properties" => builder.properties(parser.map())
                    case _ => // Handle other fields as needed
                  })
            case "properties" =>
              builder.schema(parser.map())
          })
    } catch {
      case e: Exception =>
        throw new IllegalStateException("Failed to parse metadata JSON", e)
    }

    builder.indexSettings(settings)
    builder.build()
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
    private var indexSettings: String = null

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

    def addOption(key: String, value: AnyRef): this.type = {
      this.options.put(key, value)
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

    def addSchemaField(key: String, value: AnyRef): this.type = {
      schema.put(key, value)
      this
    }

    def indexSettings(indexSettings: String): this.type = {
      this.indexSettings = indexSettings
      this
    }

    // Build method to create the FlintMetadata instance
    def build(): FlintMetadata = {
      FlintMetadata(
        version,
        name,
        kind,
        source,
        indexedColumns,
        options,
        properties,
        schema,
        indexSettings)
    }
  }
}
