/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.common.model

/**
 * Provides a mutable map to store and retrieve contextual data using key-value pairs.
 */
trait ContextualDataStore {

  /** Holds the contextual data as key-value pairs. */
  var context: Map[String, Any] = Map.empty

  /**
   * Adds a key-value pair to the context map.
   */
  def setContextValue(key: String, value: Any): Unit = {
    context += (key -> value)
  }

  /**
   * Retrieves the value associated with a key from the context map.
   */
  def getContextValue(key: String): Option[Any] = {
    context.get(key)
  }
}
