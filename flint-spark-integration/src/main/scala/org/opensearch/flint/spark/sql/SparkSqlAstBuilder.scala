/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.sql

import java.util.Locale

import scala.collection.JavaConverters.asScalaBufferConverter

import org.opensearch.flint.spark.FlintSparkIndexOptions
import org.opensearch.flint.spark.sql.FlintSparkSqlExtensionsParser.{PropertyKeyContext, PropertyListContext, PropertyValueContext}

import org.apache.spark.sql.catalyst.parser.ParserUtils.string

/**
 * AST builder that builds for common rule in Spark SQL grammar.
 */
trait SparkSqlAstBuilder extends FlintSparkSqlExtensionsVisitor[AnyRef] {

  override def visitPropertyList(ctx: PropertyListContext): FlintSparkIndexOptions = {
    val properties = ctx.property.asScala.map { property =>
      val key = visitPropertyKey(property.key)
      val value = visitPropertyValue(property.value)
      key -> value
    }
    FlintSparkIndexOptions(properties.toMap)
  }

  override def visitPropertyKey(key: PropertyKeyContext): String = {
    if (key.STRING() != null) {
      string(key.STRING())
    } else {
      key.getText
    }
  }

  override def visitPropertyValue(value: PropertyValueContext): String = {
    if (value == null) {
      null
    } else if (value.STRING != null) {
      string(value.STRING)
    } else if (value.booleanValue != null) {
      value.getText.toLowerCase(Locale.ROOT)
    } else {
      value.getText
    }
  }
}
