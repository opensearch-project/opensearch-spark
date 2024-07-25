/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.table

/**
 * Schema in OpenSearch index mapping format.
 *
 * @param jsonSchema
 */
case class JsonSchema(jsonSchema: String) extends Schema {
  override def asJson(): String = jsonSchema
}
