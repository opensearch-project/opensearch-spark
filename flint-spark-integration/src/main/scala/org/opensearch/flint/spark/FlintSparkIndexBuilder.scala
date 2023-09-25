/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import org.apache.spark.sql.catalog.Column

/**
 * Flint Spark index builder base class.
 *
 * @param flint
 *   Flint Spark API entrypoint
 */
abstract class FlintSparkIndexBuilder(flint: FlintSpark) {

  /** Source table name */
  protected var tableName: String = ""

  /** All columns of the given source table */
  lazy protected val allColumns: Map[String, Column] = {
    require(tableName.nonEmpty, "Source table name is not provided")

    flint.spark.catalog
      .listColumns(tableName)
      .collect()
      .map(col => (col.name, col))
      .toMap
  }

  /**
   * Create Flint index.
   *
   * @param ignoreIfExists
   *   ignore existing index
   */
  def create(ignoreIfExists: Boolean = false): Unit =
    flint.createIndex(buildIndex(), ignoreIfExists)

  /**
   * Build method for concrete builder class to implement
   */
  protected def buildIndex(): FlintSparkIndex

  protected def findColumn(colName: String): Column =
    allColumns.getOrElse(
      colName,
      throw new IllegalArgumentException(s"Column $colName does not exist"))
}
