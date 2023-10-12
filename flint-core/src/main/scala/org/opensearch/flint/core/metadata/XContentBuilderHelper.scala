/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.metadata

import java.nio.charset.StandardCharsets.UTF_8

import org.opensearch.common.bytes.BytesReference
import org.opensearch.common.xcontent._
import org.opensearch.common.xcontent.json.JsonXContent

object XContentBuilderHelper {

  def buildJson(block: XContentBuilder => Unit): String = {
    val builder: XContentBuilder = XContentFactory.jsonBuilder
    builder.startObject
    block(builder)
    builder.endObject()
    BytesReference.bytes(builder).utf8ToString
  }

  def objectField(builder: XContentBuilder, name: String)(block: => Unit): Unit = {
    builder.startObject(name)
    block
    builder.endObject()
  }

  def optionalObjectField(builder: XContentBuilder, name: String, value: AnyRef): Unit = {
    if (value == null) {
      builder.startObject(name).endObject()
    } else {
      builder.field(name, value)
    }
  }

  def createJsonParser(json: String): XContentParser = {
    JsonXContent.jsonXContent.createParser(
      NamedXContentRegistry.EMPTY,
      DeprecationHandler.IGNORE_DEPRECATIONS,
      json.getBytes(UTF_8))
  }

  def parseJson(json: String)(block: (XContentParser, String) => Unit): Unit = {
    val parser = createJsonParser(json)

    // Read first root object token and start parsing
    parser.nextToken()
    parseObjectField(parser)(block)
  }

  def parseObjectField(parser: XContentParser)(block: (XContentParser, String) => Unit): Unit = {
    while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
      val fieldName: String = parser.currentName()
      parser.nextToken() // Move to the field value

      block(parser, fieldName)
    }
  }

  def parseArrayField(parser: XContentParser)(block: => Unit): Unit = {
    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
      block
    }
  }
}
