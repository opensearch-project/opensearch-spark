/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import scala.collection.JavaConverters._

import org.json4s.{Formats, NoTypeHints}
import org.json4s.native.Serialization
import org.opensearch.flint.core.{FlintClient, FlintClientBuilder}
import org.opensearch.flint.core.metadata.log.FlintMetadataLogEntry.IndexState._
import org.opensearch.flint.core.metadata.log.OptimisticTransaction.NO_LOG_ENTRY
import org.opensearch.flint.spark.FlintSparkIndex.ID_COLUMN
import org.opensearch.flint.spark.FlintSparkIndexOptions.OptionName._
import org.opensearch.flint.spark.covering.FlintSparkCoveringIndex
import org.opensearch.flint.spark.mv.FlintSparkMaterializedView
import org.opensearch.flint.spark.refresh.FlintSparkIndexRefresh
import org.opensearch.flint.spark.refresh.FlintSparkIndexRefresh.RefreshMode._
import org.opensearch.flint.spark.skipping.FlintSparkSkippingIndex
import org.opensearch.flint.spark.skipping.FlintSparkSkippingStrategy.SkippingKindSerializer
import org.opensearch.flint.spark.skipping.recommendations.DataTypeSkippingStrategy

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.flint.FlintDataSourceV2.FLINT_DATASOURCE
import org.apache.spark.sql.flint.config.FlintSparkConf
import org.apache.spark.sql.flint.config.FlintSparkConf.{DOC_ID_COLUMN_NAME, IGNORE_DOC_ID_COLUMN}

/**
 * Flint Spark integration API entrypoint.
 */
class FlintSpark(val spark: SparkSession) extends Logging {

  /** Flint spark configuration */
  private val flintSparkConf: FlintSparkConf =
    FlintSparkConf(
      Map(
        DOC_ID_COLUMN_NAME.optionKey -> ID_COLUMN,
        IGNORE_DOC_ID_COLUMN.optionKey -> "true").asJava)

  /** Flint client for low-level index operation */
  private val flintClient: FlintClient = FlintClientBuilder.build(flintSparkConf.flintOptions())

  /** Required by json4s parse function */
  implicit val formats: Formats = Serialization.formats(NoTypeHints) + SkippingKindSerializer

  /**
   * Data source name. Assign empty string in case of backward compatibility. TODO: remove this in
   * future
   */
  private val dataSourceName: String =
    spark.conf.getOption("spark.flint.datasource.name").getOrElse("")

  /** Flint Spark index monitor */
  val flintIndexMonitor: FlintSparkIndexMonitor =
    new FlintSparkIndexMonitor(spark, flintClient, dataSourceName)

  /**
   * Create index builder for creating index with fluent API.
   *
   * @return
   *   index builder
   */
  def skippingIndex(): FlintSparkSkippingIndex.Builder = {
    new FlintSparkSkippingIndex.Builder(this)
  }

  /**
   * Create index builder for creating index with fluent API.
   *
   * @return
   *   index builder
   */
  def coveringIndex(): FlintSparkCoveringIndex.Builder = {
    new FlintSparkCoveringIndex.Builder(this)
  }

  /**
   * Create materialized view builder for creating mv with fluent API.
   *
   * @return
   *   mv builder
   */
  def materializedView(): FlintSparkMaterializedView.Builder = {
    new FlintSparkMaterializedView.Builder(this)
  }

  /**
   * Create the given index with metadata.
   *
   * @param index
   *   Flint index to create
   * @param ignoreIfExists
   *   Ignore existing index
   */
  def createIndex(index: FlintSparkIndex, ignoreIfExists: Boolean = false): Unit = {
    logInfo(s"Creating Flint index $index with ignoreIfExists $ignoreIfExists")
    val indexName = index.name()
    if (flintClient.exists(indexName)) {
      if (!ignoreIfExists) {
        throw new IllegalStateException(s"Flint index $indexName already exists")
      }
    } else {
      val metadata = index.metadata()
      try {
        flintClient
          .startTransaction(indexName, dataSourceName, true)
          .initialLog(latest => latest.state == EMPTY || latest.state == DELETED)
          .transientLog(latest => latest.copy(state = CREATING))
          .finalLog(latest => latest.copy(state = ACTIVE))
          .commit(latest =>
            if (latest == null) { // in case transaction capability is disabled
              flintClient.createIndex(indexName, metadata)
            } else {
              logInfo(s"Creating index with metadata log entry ID ${latest.id}")
              flintClient.createIndex(indexName, metadata.copy(latestId = Some(latest.id)))
            })
        logInfo("Create index complete")
      } catch {
        case e: Exception =>
          logError("Failed to create Flint index", e)
          throw new IllegalStateException("Failed to create Flint index")
      }
    }
  }

  /**
   * Start refreshing index data according to the given mode.
   *
   * @param indexName
   *   index name
   * @return
   *   refreshing job ID (empty if batch job for now)
   */
  def refreshIndex(indexName: String): Option[String] = {
    logInfo(s"Refreshing Flint index $indexName")
    val index = describeIndex(indexName)
      .getOrElse(throw new IllegalStateException(s"Index $indexName doesn't exist"))
    val indexRefresh = FlintSparkIndexRefresh.create(indexName, index)

    try {
      flintClient
        .startTransaction(indexName, dataSourceName)
        .initialLog(latest => latest.state == ACTIVE)
        .transientLog(latest =>
          latest.copy(state = REFRESHING, createTime = System.currentTimeMillis()))
        .finalLog(latest => {
          // Change state to active if full, otherwise update index state regularly
          if (indexRefresh.refreshMode == AUTO) {
            logInfo("Scheduling index state monitor")
            flintIndexMonitor.startMonitor(indexName)
            latest
          } else {
            logInfo("Updating index state to active")
            latest.copy(state = ACTIVE)
          }
        })
        .commit(_ => indexRefresh.start(spark, flintSparkConf))
    } catch {
      case e: Exception =>
        logError("Failed to refresh Flint index", e)
        throw new IllegalStateException("Failed to refresh Flint index")
    }
  }

  /**
   * Describe all Flint indexes whose name matches the given pattern.
   *
   * @param indexNamePattern
   *   index name pattern which may contains wildcard
   * @return
   *   Flint index list
   */
  def describeIndexes(indexNamePattern: String): Seq[FlintSparkIndex] = {
    logInfo(s"Describing indexes with pattern $indexNamePattern")
    if (flintClient.exists(indexNamePattern)) {
      flintClient
        .getAllIndexMetadata(indexNamePattern)
        .asScala
        .flatMap(FlintSparkIndexFactory.create)
    } else {
      Seq.empty
    }
  }

  /**
   * Describe a Flint index.
   *
   * @param indexName
   *   index name
   * @return
   *   Flint index
   */
  def describeIndex(indexName: String): Option[FlintSparkIndex] = {
    logInfo(s"Describing index name $indexName")
    if (flintClient.exists(indexName)) {
      val metadata = flintClient.getIndexMetadata(indexName)
      FlintSparkIndexFactory.create(metadata)
    } else {
      Option.empty
    }
  }

  /**
   * Update the given index with metadata and update associated job.
   *
   * @param index
   *   Flint index to update
   * @param updateMode
   *   update mode
   * @return
   *   refreshing job ID (empty if no job)
   */
  def updateIndex(index: FlintSparkIndex): Option[String] = {
    logInfo(s"Updating Flint index $index")
    val indexName = index.name

    validateUpdateAllowed(
      describeIndex(indexName)
        .getOrElse(throw new IllegalStateException(s"Index $indexName doesn't exist"))
        .options,
      index.options)

    try {
      // Relies on validation to forbid auto-to-auto and manual-to-manual updates
      index.options.autoRefresh() match {
        case true => updateIndexManualToAuto(index)
        case false => updateIndexAutoToManual(index)
      }
    } catch {
      case e: Exception =>
        logError("Failed to update Flint index", e)
        throw new IllegalStateException("Failed to update Flint index")
    }
  }

  /**
   * Delete index and refreshing job associated.
   *
   * @param indexName
   *   index name
   * @return
   *   true if exist and deleted, otherwise false
   */
  def deleteIndex(indexName: String): Boolean = {
    logInfo(s"Deleting Flint index $indexName")
    if (flintClient.exists(indexName)) {
      try {
        flintClient
          .startTransaction(indexName, dataSourceName)
          .initialLog(latest => latest.state == ACTIVE || latest.state == REFRESHING)
          .transientLog(latest => latest.copy(state = DELETING))
          .finalLog(latest => latest.copy(state = DELETED))
          .commit(_ => {
            // TODO: share same transaction for now
            flintIndexMonitor.stopMonitor(indexName)
            stopRefreshingJob(indexName)
            true
          })
      } catch {
        case e: Exception =>
          logError("Failed to delete Flint index", e)
          throw new IllegalStateException("Failed to delete Flint index")
      }
    } else {
      logInfo("Flint index to be deleted doesn't exist")
      false
    }
  }

  /**
   * Delete a Flint index physically.
   *
   * @param indexName
   *   index name
   * @return
   *   true if exist and deleted, otherwise false
   */
  def vacuumIndex(indexName: String): Boolean = {
    logInfo(s"Vacuuming Flint index $indexName")
    if (flintClient.exists(indexName)) {
      try {
        flintClient
          .startTransaction(indexName, dataSourceName)
          .initialLog(latest => latest.state == DELETED)
          .transientLog(latest => latest.copy(state = VACUUMING))
          .finalLog(_ => NO_LOG_ENTRY)
          .commit(_ => {
            flintClient.deleteIndex(indexName)
            true
          })
      } catch {
        case e: Exception =>
          logError("Failed to vacuum Flint index", e)
          throw new IllegalStateException("Failed to vacuum Flint index")
      }
    } else {
      logInfo("Flint index to vacuum doesn't exist")
      false
    }
  }

  /**
   * Recover index job.
   *
   * @param indexName
   *   index name
   */
  def recoverIndex(indexName: String): Boolean = {
    logInfo(s"Recovering Flint index $indexName")
    val index = describeIndex(indexName)
    if (index.exists(_.options.autoRefresh())) {
      try {
        flintClient
          .startTransaction(indexName, dataSourceName)
          .initialLog(latest => Set(ACTIVE, REFRESHING, FAILED).contains(latest.state))
          .transientLog(latest =>
            latest.copy(state = RECOVERING, createTime = System.currentTimeMillis()))
          .finalLog(latest => {
            flintIndexMonitor.startMonitor(indexName)
            latest.copy(state = REFRESHING)
          })
          .commit(_ =>
            FlintSparkIndexRefresh
              .create(indexName, index.get)
              .start(spark, flintSparkConf))

        logInfo("Recovery complete")
        true
      } catch {
        case e: Exception =>
          logError("Failed to recover Flint index", e)
          throw new IllegalStateException("Failed to recover Flint index")
      }
    } else {
      logInfo("Index to be recovered either doesn't exist or not auto refreshed")
      if (index.isEmpty) {
        /*
         * If execution reaches this point, it indicates that the Flint index is corrupted.
         * In such cases, clean up the metadata log, as the index data no longer exists.
         * There is a very small possibility that users may recreate the index in the
         * interim, but metadata log get deleted by this cleanup process.
         */
        logWarning("Cleaning up metadata log as index data has been deleted")
        flintClient
          .startTransaction(indexName, dataSourceName)
          .initialLog(_ => true)
          .finalLog(_ => NO_LOG_ENTRY)
          .commit(_ => {})
      }
      false
    }
  }

  /**
   * Build data frame for querying the given index. This is mostly for unit test convenience.
   *
   * @param indexName
   *   index name
   * @return
   *   index query data frame
   */
  def queryIndex(indexName: String): DataFrame = {
    spark.read.format(FLINT_DATASOURCE).load(indexName)
  }

  /**
   * Recommend skipping index columns and algorithm.
   *
   * @param tableName
   *   table name
   * @return
   *   skipping index recommendation dataframe
   */
  def analyzeSkippingIndex(tableName: String): Seq[Row] = {
    new DataTypeSkippingStrategy().analyzeSkippingIndexColumns(tableName, spark)
  }

  private def stopRefreshingJob(indexName: String): Unit = {
    logInfo(s"Terminating refreshing job $indexName")
    val job = spark.streams.active.find(_.name == indexName)
    if (job.isDefined) {
      job.get.stop()
    } else {
      logWarning("Refreshing job not found")
    }
  }

  /**
   * Validate the index update options are allowed.
   * @param originalOptions
   *   original options
   * @param updatedOptions
   *   the updated options
   */
  private def validateUpdateAllowed(
      originalOptions: FlintSparkIndexOptions,
      updatedOptions: FlintSparkIndexOptions): Unit = {
    // auto_refresh must change
    if (updatedOptions.autoRefresh() == originalOptions.autoRefresh()) {
      throw new IllegalArgumentException("auto_refresh option must be updated")
    }

    val refreshMode = (updatedOptions.autoRefresh(), updatedOptions.incrementalRefresh()) match {
      case (true, false) => AUTO
      case (false, false) => FULL
      case (false, true) => INCREMENTAL
    }

    // validate allowed options depending on refresh mode
    val allowedOptionNames = refreshMode match {
      case FULL => Set(AUTO_REFRESH, INCREMENTAL_REFRESH)
      case AUTO | INCREMENTAL =>
        Set(
          AUTO_REFRESH,
          INCREMENTAL_REFRESH,
          REFRESH_INTERVAL,
          CHECKPOINT_LOCATION,
          WATERMARK_DELAY)
    }

    // Get the changed option names
    val updateOptionNames = updatedOptions.options.filterNot { case (k, v) =>
      originalOptions.options.get(k).contains(v)
    }.keys
    if (!updateOptionNames.forall(allowedOptionNames.map(_.toString).contains)) {
      throw new IllegalArgumentException(
        s"Altering index to ${refreshMode} refresh only allows options: ${allowedOptionNames}")
    }
  }

  private def updateIndexAutoToManual(index: FlintSparkIndex): Option[String] = {
    val indexName = index.name
    val indexLogEntry = index.latestLogEntry.get
    flintClient
      .startTransaction(indexName, dataSourceName)
      .initialLog(latest =>
        latest.state == REFRESHING && latest.seqNo == indexLogEntry.seqNo && latest.primaryTerm == indexLogEntry.primaryTerm)
      .transientLog(latest => latest.copy(state = UPDATING))
      .finalLog(latest => latest.copy(state = ACTIVE))
      .commit(_ => {
        flintClient.updateIndex(indexName, index.metadata)
        logInfo("Update index options complete")
        flintIndexMonitor.stopMonitor(indexName)
        stopRefreshingJob(indexName)
        None
      })
  }

  private def updateIndexManualToAuto(index: FlintSparkIndex): Option[String] = {
    val indexName = index.name
    val indexLogEntry = index.latestLogEntry.get
    val indexRefresh = FlintSparkIndexRefresh.create(indexName, index)
    flintClient
      .startTransaction(indexName, dataSourceName)
      .initialLog(latest =>
        latest.state == ACTIVE && latest.seqNo == indexLogEntry.seqNo && latest.primaryTerm == indexLogEntry.primaryTerm)
      .transientLog(latest =>
        latest.copy(state = UPDATING, createTime = System.currentTimeMillis()))
      .finalLog(latest => {
        logInfo("Scheduling index state monitor")
        flintIndexMonitor.startMonitor(indexName)
        latest.copy(state = REFRESHING)
      })
      .commit(_ => {
        flintClient.updateIndex(indexName, index.metadata)
        logInfo("Update index options complete")
        indexRefresh.start(spark, flintSparkConf)
      })
  }
}
