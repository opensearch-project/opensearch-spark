/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import scala.collection.JavaConverters.mapAsJavaMapConverter

import org.opensearch.flint.spark.FlintSparkIndexOptions.empty

import org.apache.spark.sql.catalog.Column
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.flint.{findField, loadTable, parseTableName, qualifyTableName}
import org.apache.spark.sql.types.{StructField, StructType}

/**
 * Flint Spark index builder base class.
 *
 * @param flint
 *   Flint Spark API entrypoint
 */
abstract class FlintSparkIndexBuilder(flint: FlintSpark) {

  /** Qualified source table name */
  protected var qualifiedTableName: String = ""

  /** Index options */
  protected var indexOptions: FlintSparkIndexOptions = empty

  /** All columns of the given source table */
  lazy protected val allColumns: StructType = {
    require(qualifiedTableName.nonEmpty, "Source table name is not provided")

    val (catalog, ident) = parseTableName(flint.spark, qualifiedTableName)
    val table = loadTable(catalog, ident).getOrElse(
      throw new IllegalStateException(s"Table $qualifiedTableName is not found"))

    table.schema()
  }

  /**
   * Add index options.
   *
   * @param options
   *   index options
   * @return
   *   builder
   */
  def options(options: FlintSparkIndexOptions): this.type = {
    this.indexOptions = options
    this
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
   * Copy Flint index with updated options.
   *
   * @param index
   *   Flint index to copy
   * @param updateOptions
   *   options to update
   * @return
   *   updated Flint index
   */
  def copyWithUpdate(
      index: FlintSparkIndex,
      updateOptions: FlintSparkIndexOptions): FlintSparkIndex = {
    val originalOptions = index.options
    val updatedOptions =
      originalOptions.copy(options = originalOptions.options ++ updateOptions.options)
    val updatedMetadata = index
      .metadata()
      .copy(options = updatedOptions.options.mapValues(_.asInstanceOf[AnyRef]).asJava)
    FlintSparkIndexFactory.create(updatedMetadata).get
  }

  /**
   * Build method for concrete builder class to implement
   */
  protected def buildIndex(): FlintSparkIndex

  /**
   * Table name setter that qualifies given table name for subclass automatically.
   */
  protected def tableName_=(tableName: String): Unit = {
    qualifiedTableName = qualifyTableName(flint.spark, tableName)
  }

  /**
   * Table name getter
   */
  protected def tableName: String = {
    qualifiedTableName
  }

  /**
   * Find column with the given name.
   */
  protected def findColumn(colName: String): Column =
    findField(allColumns, colName)
      .map(field => convertFieldToColumn(colName, field))
      .getOrElse(throw new IllegalArgumentException(s"Column $colName does not exist"))

  private def convertFieldToColumn(colName: String, field: StructField): Column = {
    // Ref to CatalogImpl.listColumns(): Varchar/Char is StringType with real type name in metadata
    new Column(
      name = colName,
      description = field.getComment().orNull,
      dataType =
        CharVarcharUtils.getRawType(field.metadata).getOrElse(field.dataType).catalogString,
      nullable = field.nullable,
      isPartition = false, // useless for now so just set to false
      isBucket = false)
  }
}
