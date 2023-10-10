/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.metadata;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.opensearch.flint.core.FlintVersion.current;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.xcontent.DeprecationHandler;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.flint.core.FlintVersion;

/**
 * Flint metadata follows Flint index specification and defines metadata
 * for a Flint index regardless of query engine integration and storage.
 */
public class FlintMetadata {

  /**
   * Flint specification version.
   */
  private FlintVersion version;

  /**
   * Flint index name.
   */
  private String name;

  /**
   * Flint index type.
   */
  private String kind;

  /**
   * Source that the Flint index is derived from.
   * // TODO: extend this as a complex object to store source file list in future?
   */
  private String source;

  /**
   * Flint index options.
   * // TODO: maybe move this into properties as extended field?
   */
  private Map<String, Object> options = new HashMap<>();

  /**
   * Indexed columns of the Flint index. Use LinkedHashMap to preserve ordering.
   */
  private Map<String, Object> indexedColumns = new LinkedHashMap<>();

  /**
   * Other extended fields. Value is object for complex structure.
   */
  private Map<String, Object> properties = new HashMap<>();

  /**
   * Flint index field schema.
   */
  private Map<String, Object> schema = new HashMap<>();

  // TODO: define metadata format and create strong-typed class
  private String content;

  // TODO: piggyback optional index settings and will refactor as above
  private String indexSettings;

  public FlintMetadata() {
  }

  public FlintMetadata(String content) {
    this.content = content;

    try (XContentParser parser = JsonXContent.jsonXContent.createParser(
        NamedXContentRegistry.EMPTY,
        DeprecationHandler.IGNORE_DEPRECATIONS,
        content.getBytes(UTF_8))) {
      parser.nextToken(); // Start parsing

      while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
        String fieldName = parser.currentName();
        parser.nextToken(); // Move to the field value

        if ("_meta".equals(fieldName)) {
          parseMetaField(parser);
        } else if ("properties".equals(fieldName)) {
          schema = parser.map();
        } else if ("indexSettings".equals(fieldName)) {
          indexSettings = parser.text();
        }
      }
    } catch (IOException e) {
      throw new IllegalStateException("Failed to parse metadata JSON", e);
    }
  }

  private void parseMetaField(XContentParser parser) throws IOException {
    while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
      String metaFieldName = parser.currentName();
      parser.nextToken();

      switch (metaFieldName) {
        case "version":
          version = FlintVersion.apply(parser.text());
          break;
        case "name":
          name = parser.text();
          break;
        case "kind":
          kind = parser.text();
          break;
        case "source":
          source = parser.text();
          break;
        case "indexedColumns":
          indexedColumns = parseIndexedColumns(parser);
          break;
        case "options":
          options = parser.map();
          break;
        case "properties":
          properties = parser.map();
          break;
        default:
          break;
      }
    }
  }

  private Map<String, Object> parseIndexedColumns(XContentParser parser) throws IOException {
    Map<String, Object> columns = new LinkedHashMap<>();

    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
      while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
        String columnName = parser.currentName();
        parser.nextToken();
        String columnValue = parser.text();
        columns.put(columnName, columnValue);
      }
    }
    return columns;
  }

  public FlintMetadata(String content, String indexSettings) {
    this.content = content;
    this.indexSettings = indexSettings;
  }

  public String getContent() {
    try {
      XContentBuilder builder = XContentFactory.jsonBuilder();
      // Start main object
      builder.startObject();

      // Create the "_meta" object
      builder.startObject("_meta");
      builder.field("version", (version == null) ? current().version() : version.version());
      builder.field("name", name);
      builder.field("kind", kind);
      builder.field("source", source);
      builder.array("indexedColumns", indexedColumns);

      // Optional "options" JSON object
      if (options == null) {
        builder.startObject("options").endObject();
      } else {
        builder.field("options", options);
      }

      // Optional "properties" JSON object
      if (properties == null) {
        builder.startObject("properties").endObject();
      } else {
        builder.field("properties", properties);
      }
      builder.endObject(); // End "_meta" object

      // Create "properties" (schema) object
      builder.field("properties", schema);
      builder.endObject(); // End the main object

      return BytesReference.bytes(builder).utf8ToString();
    } catch (IOException e) {
      throw new IllegalStateException("Failed to jsonify Flint metadata", e);
    }
  }

  public String getIndexSettings() {
    return indexSettings;
  }

  public void setIndexSettings(String indexSettings) {
    this.indexSettings = indexSettings;
  }

  public FlintVersion getVersion() {
    return version;
  }

  public void setVersion(FlintVersion version) {
    this.version = version;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getKind() {
    return kind;
  }

  public void setKind(String kind) {
    this.kind = kind;
  }

  public String getSource() {
    return source;
  }

  public void setSource(String source) {
    this.source = source;
  }

  public Map<String, Object> getOptions() {
    return options;
  }

  public void setOptions(Map<String, Object> options) {
    this.options = options;
  }

  public Map<String, Object> getIndexedColumns() {
    return indexedColumns;
  }

  public void setIndexedColumns(Map<String, Object> indexedColumns) {
    this.indexedColumns = indexedColumns;
  }

  public Map<String, Object> getProperties() {
    return properties;
  }

  public void setProperties(Map<String, Object> properties) {
    this.properties = properties;
  }

  public Map<String, Object> getSchema() {
    return schema;
  }

  public void setSchema(Map<String, Object> schema) {
    this.schema = schema;
  }
}
