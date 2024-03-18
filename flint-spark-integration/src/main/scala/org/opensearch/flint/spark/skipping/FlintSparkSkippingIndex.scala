/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.skipping

import scala.collection.JavaConverters.mapAsJavaMapConverter

import org.opensearch.flint.core.metadata.FlintMetadata
import org.opensearch.flint.core.metadata.log.FlintMetadataLogEntry
import org.opensearch.flint.spark._
import org.opensearch.flint.spark.FlintSparkIndex._
import org.opensearch.flint.spark.FlintSparkIndexOptions.empty
import org.opensearch.flint.spark.skipping.FlintSparkSkippingIndex.{getSkippingIndexName, FILE_PATH_COLUMN, SKIPPING_INDEX_TYPE}
import org.opensearch.flint.spark.skipping.bloomfilter.BloomFilterSkippingStrategy
import org.opensearch.flint.spark.skipping.minmax.MinMaxSkippingStrategy
import org.opensearch.flint.spark.skipping.partition.PartitionSkippingStrategy
import org.opensearch.flint.spark.skipping.valueset.ValueSetSkippingStrategy

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.dsl.expressions.DslExpression
import org.apache.spark.sql.functions.{col, input_file_name, sha1}

/**
 * Flint skipping index in Spark.
 *
 * @param tableName
 *   source table name
 * @param indexedColumns
 *   indexed column list
 * @param options
 *   index options
 * @param latestLogEntry
 *   latest metadata log entry for index
 */
case class FlintSparkSkippingIndex(
    tableName: String,
    indexedColumns: Seq[FlintSparkSkippingStrategy],
    override val options: FlintSparkIndexOptions = empty,
    override val latestLogEntry: Option[FlintMetadataLogEntry] = None)
    extends FlintSparkIndex {

  require(indexedColumns.nonEmpty, "indexed columns must not be empty")

  /** Skipping index type */
  override val kind: String = SKIPPING_INDEX_TYPE

  override def name(): String = {
    getSkippingIndexName(tableName)
  }

  override def metadata(): FlintMetadata = {
    val indexColumnMaps =
      indexedColumns
        .map(col =>
          Map[String, AnyRef](
            "kind" -> col.kind.toString,
            "parameters" -> col.parameters.asJava,
            "columnName" -> col.columnName,
            "columnType" -> col.columnType).asJava)
        .toArray

    val fieldTypes =
      indexedColumns
        .flatMap(_.outputSchema())
        .toMap + (FILE_PATH_COLUMN -> "string")
    val schemaJson = generateSchemaJSON(fieldTypes)

    metadataBuilder(this)
      .name(name())
      .source(tableName)
      .indexedColumns(indexColumnMaps)
      .schema(schemaJson)
      .build()
  }

  override def build(spark: SparkSession, df: Option[DataFrame]): DataFrame = {
    val outputNames = indexedColumns.flatMap(_.outputSchema().keys)
    val aggFuncs = indexedColumns.flatMap(_.getAggregators)

    // Wrap aggregate function with output column name
    val namedAggFuncs =
      (outputNames, aggFuncs).zipped.map { case (name, aggFunc) =>
        new Column(aggFunc.as(name))
      }

    df.getOrElse(spark.read.table(quotedTableName(tableName)))
      .groupBy(input_file_name().as(FILE_PATH_COLUMN))
      .agg(namedAggFuncs.head, namedAggFuncs.tail: _*)
      .withColumn(ID_COLUMN, sha1(col(FILE_PATH_COLUMN)))
  }
}

object FlintSparkSkippingIndex {

  /** Index type name */
  val SKIPPING_INDEX_TYPE = "skipping"

  /** File path column name */
  val FILE_PATH_COLUMN = "file_path"

  /** Flint skipping index name suffix */
  val SKIPPING_INDEX_SUFFIX = "skipping_index"

  /**
   * Get skipping index name which follows the convention: "flint_" prefix + source table name +
   * "_skipping_index" suffix.
   *
   * This helps identify the Flint index because Flint index is not registered to Spark Catalog
   * for now.
   *
   * @param tableName
   *   full table name
   * @return
   *   Flint skipping index name
   */
  def getSkippingIndexName(tableName: String): String = {
    require(
      tableName.split("\\.").length >= 3,
      "Qualified table name catalog.database.table is required")

    flintIndexNamePrefix(tableName) + "_" + SKIPPING_INDEX_SUFFIX
  }

  /** Builder class for skipping index build */
  class Builder(flint: FlintSpark) extends FlintSparkIndexBuilder(flint) {
    private var indexedColumns: Seq[FlintSparkSkippingStrategy] = Seq()

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
     * Add partition skipping indexed columns.
     *
     * @param colNames
     *   indexed column names
     * @return
     *   index builder
     */
    def addPartitions(colNames: String*): Builder = {
      require(tableName.nonEmpty, "table name cannot be empty")

      colNames
        .map(findColumn)
        .map(col => PartitionSkippingStrategy(columnName = col.name, columnType = col.dataType))
        .foreach(addIndexedColumn)
      this
    }

    /**
     * Add value set skipping indexed column.
     *
     * @param colName
     *   indexed column name
     * @param params
     *   value set parameters
     * @return
     *   index builder
     */
    def addValueSet(colName: String, params: Map[String, String] = Map.empty): Builder = {
      require(tableName.nonEmpty, "table name cannot be empty")

      val col = findColumn(colName)
      addIndexedColumn(
        ValueSetSkippingStrategy(
          columnName = col.name,
          columnType = col.dataType,
          params = params))
      this
    }

    /**
     * Add min max skipping indexed column.
     *
     * @param colName
     *   indexed column name
     * @return
     *   index builder
     */
    def addMinMax(colName: String): Builder = {
      val col = findColumn(colName)
      indexedColumns =
        indexedColumns :+ MinMaxSkippingStrategy(columnName = col.name, columnType = col.dataType)
      this
    }

    /**
     * Add bloom filter skipping index column.
     *
     * @param colName
     *   indexed column name
     * @param params
     *   bloom filter parameters
     * @return
     *   index builder
     */
    def addBloomFilter(colName: String, params: Map[String, String] = Map.empty): Builder = {
      val col = findColumn(colName)
      indexedColumns = indexedColumns :+ BloomFilterSkippingStrategy(
        columnName = col.name,
        columnType = col.dataType,
        params = params)
      this
    }

    override def buildIndex(): FlintSparkIndex =
      new FlintSparkSkippingIndex(tableName, indexedColumns, indexOptions)

    private def addIndexedColumn(indexedCol: FlintSparkSkippingStrategy): Unit = {
      require(
        indexedColumns.forall(_.columnName != indexedCol.columnName),
        s"${indexedCol.columnName} is already indexed")

      indexedColumns = indexedColumns :+ indexedCol
    }
  }
}
