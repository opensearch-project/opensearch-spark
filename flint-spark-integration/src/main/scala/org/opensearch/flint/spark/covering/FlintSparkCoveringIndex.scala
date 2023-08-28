/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.covering

import org.opensearch.flint.core.metadata.FlintMetadata
import org.opensearch.flint.spark.{FlintSpark, FlintSparkIndex, FlintSparkIndexBuilder}
import org.opensearch.flint.spark.covering.FlintSparkCoveringIndex.COVERING_INDEX_TYPE

import org.apache.spark.sql.DataFrame

class FlintSparkCoveringIndex(
    override val name: String,
    val tableName: String,
    val indexedColumns: Map[String, String])
    extends FlintSparkIndex {

  require(indexedColumns.nonEmpty, "indexed columns must not be empty")

  override val kind: String = COVERING_INDEX_TYPE

  override def metadata(): FlintMetadata = {
    new FlintMetadata(s"""{
        |   "_meta": {
        |     "kind": "$kind",
        |     "indexedColumns": $indexedColumns,
        |     "source": "$tableName"
        |   },
        |   "properties": $getSchema
        | }
        |""".stripMargin)
  }

  override def build(df: DataFrame): DataFrame = {
    null
  }

  private def getSchema: String = {
    ""
  }
}

object FlintSparkCoveringIndex {

  /** Covering index type name */
  val COVERING_INDEX_TYPE = "covering"

  /** Builder class for covering index build */
  class Builder(flint: FlintSpark) extends FlintSparkIndexBuilder(flint) {
    private var indexName: String = ""
    private var indexedColumns: Map[String, String] = Map()

    /**
     * Set covering index name.
     *
     * @param indexName
     *   index name
     * @return
     *   index builder
     */
    def indexName(indexName: String): Builder = {
      this.indexName = indexName
      this
    }

    /**
     * Configure which source table the index is based on.
     *
     * @param tableName
     *   full table name
     * @return
     *   index builder
     */
    def onTable(tableName: String): Builder = {
      this.tableName = tableName
      this
    }

    /**
     * Add indexed column name.
     *
     * @param colName
     *   column name
     * @return
     *   index builder
     */
    def addIndexColumn(colName: String): Builder = {
      indexedColumns += (colName -> findColumn(colName).dataType)
      this
    }

    override protected def buildIndex(): FlintSparkIndex =
      new FlintSparkCoveringIndex(indexName, tableName, indexedColumns)
  }
}
