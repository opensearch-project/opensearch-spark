/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.covering

import org.json4s.{Formats, NoTypeHints}
import org.json4s.JsonAST.{JArray, JObject, JString}
import org.json4s.native.JsonMethods.{compact, parse, render}
import org.json4s.native.Serialization
import org.opensearch.flint.core.metadata.FlintMetadata
import org.opensearch.flint.spark.{FlintSpark, FlintSparkIndex, FlintSparkIndexBuilder, FlintSparkIndexOptions}
import org.opensearch.flint.spark.FlintSparkIndex.flintIndexNamePrefix
import org.opensearch.flint.spark.FlintSparkIndexOptions.empty
import org.opensearch.flint.spark.covering.FlintSparkCoveringIndex.{getFlintIndexName, COVERING_INDEX_TYPE}

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.flint.datatype.FlintDataType
import org.apache.spark.sql.types.StructType

/**
 * Flint covering index in Spark.
 *
 * @param indexName
 *   index name
 * @param tableName
 *   source table name
 * @param indexedColumns
 *   indexed column list
 */
case class FlintSparkCoveringIndex(
    indexName: String,
    tableName: String,
    indexedColumns: Map[String, String],
    override val options: FlintSparkIndexOptions = empty)
    extends FlintSparkIndex {

  require(indexedColumns.nonEmpty, "indexed columns must not be empty")

  /** Required by json4s write function */
  implicit val formats: Formats = Serialization.formats(NoTypeHints)

  override val kind: String = COVERING_INDEX_TYPE

  override def name(): String = getFlintIndexName(indexName, tableName)

  override def metadata(): FlintMetadata = {
    new FlintMetadata(s"""{
        |   "_meta": {
        |     "name": "$indexName",
        |     "kind": "$kind",
        |     "indexedColumns": $getMetaInfo,
        |     "source": "$tableName",
        |     "options": $getIndexOptions
        |   },
        |   "properties": $getSchema
        | }
        |""".stripMargin)
  }

  override def build(df: DataFrame): DataFrame = {
    val colNames = indexedColumns.keys.toSeq
    df.select(colNames.head, colNames.tail: _*)
  }

  // TODO: refactor all these once Flint metadata spec finalized
  private def getMetaInfo: String = {
    val objects = indexedColumns.map { case (colName, colType) =>
      JObject("columnName" -> JString(colName), "columnType" -> JString(colType))
    }.toList
    Serialization.write(JArray(objects))
  }

  private def getIndexOptions: String = {
    Serialization.write(options.options)
  }

  private def getSchema: String = {
    val catalogDDL =
      indexedColumns
        .map { case (colName, colType) => s"$colName $colType not null" }
        .mkString(",")
    val properties = FlintDataType.serialize(StructType.fromDDL(catalogDDL))
    compact(render(parse(properties) \ "properties"))
  }
}

object FlintSparkCoveringIndex {

  /** Covering index type name */
  val COVERING_INDEX_TYPE = "covering"

  /** Flint covering index name suffix */
  val COVERING_INDEX_SUFFIX = "_index"

  /**
   * Get Flint index name which follows the convention: "flint_" prefix + source table name + +
   * given index name + "_index" suffix.
   *
   * This helps identify the Flint index because Flint index is not registered to Spark Catalog
   * for now.
   *
   * @param tableName
   *   full table name
   * @param indexName
   *   index name specified by user
   * @return
   *   Flint covering index name
   */
  def getFlintIndexName(indexName: String, tableName: String): String = {
    require(tableName.contains("."), "Full table name database.table is required")

    flintIndexNamePrefix(tableName) + indexName + COVERING_INDEX_SUFFIX
  }

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
    def name(indexName: String): Builder = {
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
     * @param colNames
     *   column names
     * @return
     *   index builder
     */
    def addIndexColumns(colNames: String*): Builder = {
      colNames.foreach(colName => {
        indexedColumns += (colName -> findColumn(colName).dataType)
      })
      this
    }

    override protected def buildIndex(): FlintSparkIndex =
      new FlintSparkCoveringIndex(indexName, tableName, indexedColumns, indexOptions)
  }
}
