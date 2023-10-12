/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.metadata

import org.opensearch.common.bytes.BytesReference
import org.opensearch.common.xcontent.{XContentBuilder, XContentFactory}

object XContentBuilderHelper {

  def buildJson(block: XContentBuilder => Unit): String = {
    val builder: XContentBuilder = XContentFactory.jsonBuilder
    builder.startObject
    block(builder)
    builder.endObject()
    BytesReference.bytes(builder).utf8ToString
  }

  def objectField(builder: XContentBuilder, name: String, block: => Unit): Unit = {
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
}
