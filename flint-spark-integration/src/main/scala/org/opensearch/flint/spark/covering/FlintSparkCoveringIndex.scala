/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.covering

import scala.collection.JavaConverters.mapAsJavaMapConverter

import org.opensearch.flint.core.metadata.FlintMetadata
import org.opensearch.flint.spark._
import org.opensearch.flint.spark.FlintSparkIndex._
import org.opensearch.flint.spark.FlintSparkIndexOptions.empty
import org.opensearch.flint.spark.covering.FlintSparkCoveringIndex.{getFlintIndexName, COVERING_INDEX_TYPE}

import org.apache.spark.sql._

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
    filterCondition: Option[String] = None,
    override val options: FlintSparkIndexOptions = empty)
    extends FlintSparkIndex {

  require(indexedColumns.nonEmpty, "indexed columns must not be empty")

  override val kind: String = COVERING_INDEX_TYPE

  override def name(): String = getFlintIndexName(indexName, tableName)

  override def metadata(): FlintMetadata = {
    val indexColumnMaps = {
      indexedColumns.map { case (colName, colType) =>
        Map[String, AnyRef]("columnName" -> colName, "columnType" -> colType).asJava
      }.toArray
    }
    val schemaJson = generateSchemaJSON(indexedColumns)

    val builder = metadataBuilder(this)
      .name(indexName)
      .source(tableName)
      .indexedColumns(indexColumnMaps)
      .schema(schemaJson)

    // Add optional index properties
    filterCondition.map(builder.addProperty("filterCondition", _))
    builder.build()
  }

  override def build(spark: SparkSession, df: Option[DataFrame]): DataFrame = {
    var colNames = indexedColumns.keys.toSeq
    var job = df.getOrElse(spark.read.table(tableName))

    // Add optional ID column
    val idColumn = generateIdColumn(job, options.idExpression())
    if (idColumn.isDefined) {
      logInfo(s"Generate ID column based on expression $idColumn")
      colNames = colNames :+ ID_COLUMN
      job = job.withColumn(ID_COLUMN, idColumn.get)
    } else {
      logWarning("Cannot generate ID column which may cause duplicate data when restart")
    }

    // Add optional filtering condition
    filterCondition
      .map(job.where)
      .getOrElse(job)
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
    private var indexName: String = ""
    private var indexedColumns: Map[String, String] = Map()
    private var filterCondition: Option[String] = None

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

    /**
     * Add filtering condition.
     *
     * @param condition
     *   filter condition
     * @return
     *   index builder
     */
    def filterBy(condition: String): Builder = {
      filterCondition = Some(condition)
      this
    }

    override protected def buildIndex(): FlintSparkIndex =
      new FlintSparkCoveringIndex(
        indexName,
        tableName,
        indexedColumns,
        filterCondition,
        indexOptions)
  }
}
