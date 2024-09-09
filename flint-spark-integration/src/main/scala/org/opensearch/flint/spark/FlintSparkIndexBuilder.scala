/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import java.util.Collections

import scala.collection.JavaConverters.mapAsJavaMapConverter

import org.opensearch.flint.core.scheduler.util.IntervalSchedulerParser.parseStringSchedule
import org.opensearch.flint.spark.FlintSparkIndexOptions.OptionName.CHECKPOINT_LOCATION
import org.opensearch.flint.spark.FlintSparkIndexOptions.OptionName.SCHEDULER_MODE
import org.opensearch.flint.spark.FlintSparkIndexOptions.empty
import org.opensearch.flint.spark.refresh.FlintSparkIndexRefresh
import org.opensearch.flint.spark.refresh.FlintSparkIndexRefresh.SchedulerMode

import org.apache.spark.sql.catalog.Column
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.flint.{findField, loadTable, parseTableName, qualifyTableName}
import org.apache.spark.sql.flint.config.FlintSparkConf
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
  def options(options: FlintSparkIndexOptions, indexName: String): this.type = {
    this.indexOptions = updateOptionsWithDefaults(indexName, options)
    this
  }

  /**
   * Is Flint index refresh in external scheduler mode. This only applies to auto refresh.
   *
   * @return
   *   true if external scheduler is enabled, false otherwise
   */
  def isExternalSchedulerEnabled(): Boolean = {
    this.indexOptions.isExternalSchedulerEnabled()
  }

  /**
   * Create Flint index.
   *
   * @param ignoreIfExists
   *   ignore existing index
   */
  def create(ignoreIfExists: Boolean = false): Unit =
    flint.createIndex(validateIndex(buildIndex()), ignoreIfExists)

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
    val updatedOptions = updateOptionsWithDefaults(
      index.name(),
      originalOptions.copy(options = originalOptions.options ++ updateOptions.options))
    val updatedMetadata = index
      .metadata()
      .copy(options = updatedOptions.options.mapValues(_.asInstanceOf[AnyRef]).asJava)
    validateIndex(FlintSparkIndexFactory.create(updatedMetadata).get)
  }

  /**
   * Pre-validate index to ensure its validity. By default, this method validates index options by
   * delegating to specific index refresh (index options are mostly serving index refresh).
   * Subclasses can extend this method to include additional validation logic.
   *
   * @param index
   *   Flint index to be validated
   * @return
   *   the index or exception occurred if validation failed
   */
  protected def validateIndex(index: FlintSparkIndex): FlintSparkIndex = {
    FlintSparkIndexRefresh
      .create(index.name(), index) // TODO: remove first argument?
      .validate(flint.spark)
    index
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

  /**
   * Updates the options with a default checkpoint location if not already set.
   *
   * @param indexName
   *   The index name string
   * @param options
   *   The original FlintSparkIndexOptions
   * @return
   *   Updated FlintSparkIndexOptions
   */
  private def updateOptionsWithDefaults(
      indexName: String,
      options: FlintSparkIndexOptions): FlintSparkIndexOptions = {
    val flintSparkConf = new FlintSparkConf(Collections.emptyMap[String, String])

    val updatedOptions =
      new scala.collection.mutable.HashMap[String, String]() ++= options.options

    options.checkpointLocation(indexName, flintSparkConf) match {
      case Some(location) => updatedOptions += (CHECKPOINT_LOCATION.toString -> location)
      case None => // Do nothing
    }

    // Update scheduler mode by default
    if (options.autoRefresh() && !updatedOptions.contains(SCHEDULER_MODE.toString)) {
      val externalSchedulerEnabled = flintSparkConf.isExternalSchedulerEnabled
      // If refresh interval is not set, or it is set but the interval is smaller than the threshold, use spark internal scheduler
      val shouldUseExternalScheduler = options.refreshInterval().exists { interval =>
        parseStringSchedule(
          flintSparkConf.externalSchedulerIntervalThreshold()).getInterval >= parseStringSchedule(
          interval).getInterval
      }
      if (externalSchedulerEnabled && shouldUseExternalScheduler) {
        updatedOptions += (SCHEDULER_MODE.toString -> SchedulerMode.EXTERNAL.toString)
      } else {
        updatedOptions += (SCHEDULER_MODE.toString -> SchedulerMode.INTERNAL.toString)
      }
    }

    FlintSparkIndexOptions(updatedOptions.toMap)
  }
}
