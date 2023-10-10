/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.metadata

import java.nio.charset.StandardCharsets.UTF_8
import java.util

import org.opensearch.common.bytes.BytesReference
import org.opensearch.common.xcontent._
import org.opensearch.common.xcontent.json.JsonXContent
import org.opensearch.flint.core.FlintVersion

/**
 * Flint metadata follows Flint index specification and defines metadata for a Flint index
 * regardless of query engine integration and storage.
 */
case class FlintMetadata(
    version: FlintVersion = FlintVersion.current(),
    name: String,
    kind: String,
    source: String,
    indexedColumns: util.Map[String, AnyRef] = new util.LinkedHashMap[String, AnyRef],
    options: util.Map[String, AnyRef] = new util.HashMap[String, AnyRef],
    properties: util.Map[String, AnyRef] = new util.HashMap[String, AnyRef],
    schema: util.Map[String, AnyRef] = new util.HashMap[String, AnyRef],
    indexSettings: String = null) {

  def getContent: String = {
    try {
      val builder: XContentBuilder = XContentFactory.jsonBuilder
      builder.startObject

      builder.startObject("_meta")
      builder.field(
        "version",
        if (version == null) FlintVersion.current().version else version.version)
      builder.field("name", name)
      builder.field("kind", kind)
      builder.field("source", source)
      builder.array("indexedColumns", indexedColumns)

      if (options == null) {
        builder.startObject("options").endObject()
      } else {
        builder.field("options", options)
      }

      if (properties == null) {
        builder.startObject("properties").endObject()
      } else {
        builder.field("properties", properties)
      }

      builder.endObject
      builder.field("properties", schema)
      builder.endObject
      BytesReference.bytes(builder).utf8ToString
    } catch {
      case e: Exception =>
        throw new IllegalStateException("Failed to jsonify Flint metadata", e)
    }
  }
}

object FlintMetadata {

  def fromJson(content: String, settings: String): FlintMetadata = {
    try {
      val parser: XContentParser = JsonXContent.jsonXContent.createParser(
        NamedXContentRegistry.EMPTY,
        DeprecationHandler.IGNORE_DEPRECATIONS,
        content.getBytes(UTF_8))
      parser.nextToken() // Start parsing

      val builder = new FlintMetadata.Builder()
      while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
        val fieldName: String = parser.currentName()
        parser.nextToken() // Move to the field value

        if ("_meta".equals(fieldName)) {
          parseMetaField(parser, builder)
        } else if ("properties".equals(fieldName)) {
          builder.schema(parser.map())
        }
      }
      builder.build()
    } catch {
      case e: Exception =>
        throw new IllegalStateException("Failed to parse metadata JSON", e)
    }
  }

  private def parseMetaField(parser: XContentParser, builder: Builder): Unit = {
    while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
      val metaFieldName: String = parser.currentName()
      parser.nextToken()

      metaFieldName match {
        case "version" => builder.version(FlintVersion.apply(parser.text()))
        case "name" => builder.name(parser.text())
        case "kind" => builder.kind(parser.text())
        case "source" => builder.source(parser.text())
        case "indexedColumns" => parseIndexedColumns(parser, builder)
        case "options" => builder.options(parser.map())
        case "properties" => builder.properties(parser.map())
        case _ => // Handle other fields as needed
      }
    }
  }

  private def parseIndexedColumns(parser: XContentParser, builder: Builder): Unit = {
    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
      while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
        val colName: String = parser.currentName()
        parser.nextToken()

        val colValue: String = parser.text()
        builder.addIndexedColumn(colName, colValue)
      }
    }
  }

  class Builder {
    private var version: FlintVersion = FlintVersion.current()
    private var name: String = ""
    private var kind: String = ""
    private var source: String = ""
    private var options: util.Map[String, AnyRef] = new util.HashMap[String, AnyRef]()
    private var indexedColumns: util.Map[String, AnyRef] =
      new util.LinkedHashMap[String, AnyRef]()
    private var properties: util.Map[String, AnyRef] = new util.HashMap[String, AnyRef]()
    private var schema: util.Map[String, AnyRef] = new util.HashMap[String, AnyRef]()
    private var indexSettings: String = null

    // Setters for each field
    def version(version: FlintVersion): Builder = {
      this.version = version
      this
    }

    def name(name: String): Builder = {
      this.name = name
      this
    }

    def kind(kind: String): Builder = {
      this.kind = kind
      this
    }

    def source(source: String): Builder = {
      this.source = source
      this
    }

    def options(options: util.Map[String, AnyRef]): Builder = {
      this.options = options
      this
    }

    def addOption(key: String, value: AnyRef): Builder = {
      this.options.put(key, value)
      this
    }

    def indexedColumns(indexedColumns: util.Map[String, AnyRef]): Builder = {
      this.indexedColumns = indexedColumns
      this
    }

    def addIndexedColumn(key: String, value: AnyRef): Builder = {
      indexedColumns.put(key, value)
      this
    }

    def properties(properties: util.Map[String, AnyRef]): Builder = {
      this.properties = properties
      this
    }

    def addProperty(key: String, value: AnyRef): Builder = {
      properties.put(key, value)
      this
    }

    def schema(schema: util.Map[String, AnyRef]): Builder = {
      this.schema = schema
      this
    }

    def addSchemaField(key: String, value: AnyRef): Builder = {
      schema.put(key, value)
      this
    }

    def indexSettings(indexSettings: String): Builder = {
      this.indexSettings = indexSettings
      this
    }

    // Build method to create the FlintMetadata instance
    def build(): FlintMetadata = {
      FlintMetadata(version, name, kind, source, indexedColumns, options, properties, schema, indexSettings)
    }
  }
}
