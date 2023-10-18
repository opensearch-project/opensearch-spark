/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.covering

import scala.collection.JavaConverters.mapAsJavaMapConverter

import org.opensearch.flint.core.metadata.FlintMetadata
import org.opensearch.flint.spark._
import org.opensearch.flint.spark.FlintSparkIndex.{flintIndexNamePrefix, generateSchemaJSON, metadataBuilder}
import org.opensearch.flint.spark.FlintSparkIndexOptions.empty
import org.opensearch.flint.spark.covering.FlintSparkCoveringIndex.{getFlintIndexName, COVERING_INDEX_TYPE}

import org.apache.spark.sql._

/**
 * Flint covering index in Spark.
 *
 * @param targetIndexName
 *   optional index target name
 * @param indexName
 *   index name
 * @param tableName
 *   source table name
 * @param indexedColumns
 *   indexed column list
 */
case class FlintSparkCoveringIndex(
    targetIndexName: Option[String] = None,
    indexName: String,
    tableName: String,
    indexedColumns: Map[String, String],
    override val options: FlintSparkIndexOptions = empty)
    extends FlintSparkIndex {

  require(indexedColumns.nonEmpty, "indexed columns must not be empty")

  override val kind: String = COVERING_INDEX_TYPE

  override def name(): String = getFlintIndexName(indexName, tableName)

  /**
   * @return
   * Flint target index name - index that already exist or has existing template to be created with
   */
  override def targetName(): Option[String] = {
    targetIndexName
  }

  override def metadata(): FlintMetadata = {
    val indexColumnMaps = {
      indexedColumns.map { case (colName, colType) =>
        Map[String, AnyRef]("columnName" -> colName, "columnType" -> colType).asJava
      }.toArray
    }
    val schemaJson = generateSchemaJSON(indexedColumns)

    metadataBuilder(this)
      .name(indexName)
      .source(tableName)
      .indexedColumns(indexColumnMaps)
      .schema(schemaJson)
      .build()
  }

  override def build(spark: SparkSession, df: Option[DataFrame]): DataFrame = {
    val colNames = indexedColumns.keys.toSeq
    df.getOrElse(spark.read.table(tableName))
      .select(colNames.head, colNames.tail: _*)
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
    require(
      tableName.split("\\.").length >= 3,
      "Qualified table name catalog.database.table is required")

    flintIndexNamePrefix(tableName) + indexName + COVERING_INDEX_SUFFIX
  }

  /** Builder class for covering index build */
  class Builder(flint: FlintSpark) extends FlintSparkIndexBuilder(flint) {
    private var targetIndexName: String = ""
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
     * Set covering index target name.
     *
     * @param indexName
     *   index name
     * @return
     *   index builder
     */
    def targetName(indexName: String): Builder = {
      this.targetIndexName = indexName
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
      new FlintSparkCoveringIndex(Option.apply(targetIndexName), indexName, tableName, indexedColumns, indexOptions)
  }
}
