/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import org.apache.spark.sql.catalog.Column

abstract class FlintSparkIndexBuilder(flint: FlintSpark) {

  protected var tableName: String = ""

  lazy val allColumns: Map[String, Column] = {
    require(tableName.nonEmpty, "Source table name is not provided")

    flint.spark.catalog
      .listColumns(tableName)
      .collect()
      .map(col => (col.name, col))
      .toMap
  }

  def create(): Unit = flint.createIndex(buildIndex())

  protected def buildIndex(): FlintSparkIndex

  protected def findColumn(colName: String): Column =
    allColumns.getOrElse(
      colName,
      throw new IllegalArgumentException(s"Column $colName does not exist"))
}
