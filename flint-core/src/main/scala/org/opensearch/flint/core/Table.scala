/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.table

import org.opensearch.flint.core.storage.FlintReader

/**
 * A OpenSearch Table.
 */
trait Table extends Serializable {

  /**
   * OpenSearch Table MetaData.
   *
   * @return
   *   {@link Table}
   */
  def metaData(): MetaData

  /**
   * Create OpenSearch Table Snapshot.
   *
   * @return
   *   {@link Table}
   */
  def snapshot(): Table

  /**
   * Is OpenSearch Table splittable.
   *
   * @return
   *   true if splittable, otherwise false.
   */
  def isSplittable(): Boolean = false

  /**
   * Slice OpenSearch Table.
   * @return
   *   a sequence of sliced OpenSearch Table
   */
  def slice(): Seq[Table]

  /**
   * Create Flint Reader from DSL query.
   *
   * @param query
   *   OpenSearch DSL query.
   * @return
   */
  def createReader(query: String): FlintReader

  /**
   * OpenSearch Table schema
   *
   * @return
   *   {@link Schema}
   */
  def schema(): Schema
}
