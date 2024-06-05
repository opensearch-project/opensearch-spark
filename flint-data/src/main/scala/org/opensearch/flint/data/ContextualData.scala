/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.data

/**
 * Provides a mutable map to store and retrieve contextual data using key-value pairs.
 */
trait ContextualData {

  /** Holds the contextual data as key-value pairs. */
  var context: Map[String, Any] = Map.empty

  /**
   * Adds a key-value pair to the context map.
   *
   * @param key
   *   The key under which the value is stored.
   * @param value
   *   The data value to store.
   */
  def addContext(key: String, value: Any): Unit = {
    context += (key -> value)
  }

  /**
   * Retrieves the value associated with a key from the context map.
   *
   * @param key
   *   The key whose value needs to be retrieved.
   * @return
   *   An option containing the value if it exists, None otherwise.
   */
  def getContext(key: String): Option[Any] = {
    context.get(key)
  }
}
