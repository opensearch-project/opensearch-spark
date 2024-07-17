/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.metadata

import java.nio.charset.StandardCharsets.UTF_8

import org.opensearch.common.xcontent._
import org.opensearch.common.xcontent.json.JsonXContent
import org.opensearch.core.common.bytes.BytesReference
import org.opensearch.core.xcontent.{DeprecationHandler, NamedXContentRegistry, XContentBuilder, XContentParser}

/**
 * JSON parsing and building helper.
 */
object FlintJsonHelper {

  /**
   * Build JSON by creating JSON builder and pass it to the given function.
   *
   * @param block
   *   building logic with JSON builder
   * @return
   *   JSON string
   */
  def buildJson(block: XContentBuilder => Unit): String = {
    val builder: XContentBuilder = XContentFactory.jsonBuilder
    builder.startObject
    block(builder)
    builder.endObject()
    BytesReference.bytes(builder).utf8ToString
  }

  /**
   * Add an object field of the name to the JSON builder and continue building it with the given
   * function.
   *
   * @param builder
   *   JSON builder
   * @param name
   *   field name
   * @param block
   *   building logic on the JSON field
   */
  def objectField(builder: XContentBuilder, name: String)(block: => Unit): Unit = {
    builder.startObject(name)
    block
    builder.endObject()
  }

  /**
   * Add an optional object field of the name to the JSON builder. Add an empty object field if
   * the value is null.
   *
   * @param builder
   *   JSON builder
   * @param name
   *   field name
   * @param value
   *   field value
   */
  def optionalObjectField(builder: XContentBuilder, name: String, value: AnyRef): Unit = {
    if (value == null) {
      builder.startObject(name).endObject()
    } else {
      builder.field(name, value)
    }
  }

  /**
   * Create a XContent JSON parser on the given JSON string.
   *
   * @param json
   *   JSON string
   * @return
   *   JSON parser
   */
  def createJsonParser(json: String): XContentParser = {
    JsonXContent.jsonXContent.createParser(
      NamedXContentRegistry.EMPTY,
      DeprecationHandler.IGNORE_DEPRECATIONS,
      json.getBytes(UTF_8))
  }

  /**
   * Parse the given JSON string by creating JSON parser and pass it to the parsing function.
   *
   * @param json
   *   JSON string
   * @param block
   *   parsing logic with the parser
   */
  def parseJson(json: String)(block: (XContentParser, String) => Unit): Unit = {
    val parser = createJsonParser(json)

    // Read first root object token and start parsing
    parser.nextToken()
    parseObjectField(parser)(block)
  }

  /**
   * Parse each inner field in the object field with the given parsing function.
   *
   * @param parser
   *   JSON parser
   * @param block
   *   parsing logic on each inner field
   */
  def parseObjectField(parser: XContentParser)(block: (XContentParser, String) => Unit): Unit = {
    while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
      val fieldName: String = parser.currentName()
      parser.nextToken() // Move to the field value

      block(parser, fieldName)
    }
  }

  /**
   * Parse each inner field in the array field.
   *
   * @param parser
   *   JSON parser
   * @param block
   *   parsing logic on each inner field
   */
  def parseArrayField(parser: XContentParser)(block: => Unit): Unit = {
    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
      block
    }
  }
}
