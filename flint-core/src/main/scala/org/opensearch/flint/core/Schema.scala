/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core

/**
 * Table Schema.
 */
trait Schema {

  /**
   * Return table schema as Json.
   *
   * @return
   *   schema.
   */
  def asJson(): String
}
