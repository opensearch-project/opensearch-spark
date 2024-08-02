/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import scala.collection.JavaConverters._

import org.json4s.{Formats, NoTypeHints}
import org.json4s.native.Serialization
import org.opensearch.flint.common.metadata.{FlintIndexMetadataService, FlintMetadata}
import org.opensearch.flint.common.metadata.log.FlintMetadataLogEntry.IndexState._
import org.opensearch.flint.common.metadata.log.FlintMetadataLogService
import org.opensearch.flint.common.metadata.log.OptimisticTransaction
import org.opensearch.flint.common.metadata.log.OptimisticTransaction.NO_LOG_ENTRY
import org.opensearch.flint.core.{FlintClient, FlintClientBuilder}
import org.opensearch.flint.core.metadata.FlintIndexMetadataServiceBuilder
import org.opensearch.flint.core.metadata.log.FlintMetadataLogServiceBuilder
import org.opensearch.flint.core.storage.FlintOpenSearchIndexMetadataService
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
class FlintSpark(val spark: SparkSession) extends FlintSparkTransactionSupport with Logging {

  /** Flint spark configuration */
  private val flintSparkConf: FlintSparkConf =
    FlintSparkConf(
      Map(
        DOC_ID_COLUMN_NAME.optionKey -> ID_COLUMN,
        IGNORE_DOC_ID_COLUMN.optionKey -> "true").asJava)

  /** Flint client for low-level index operation */
  private val flintClient: FlintClient = FlintClientBuilder.build(flintSparkConf.flintOptions())

  private val flintIndexMetadataService: FlintIndexMetadataService = {
    FlintIndexMetadataServiceBuilder.build(
      flintSparkConf.flintOptions(),
      spark.sparkContext.getConf)
  }

  override protected val flintMetadataLogService: FlintMetadataLogService = {
    FlintMetadataLogServiceBuilder.build(
      flintSparkConf.flintOptions(),
      spark.sparkContext.getConf)
  }

  /** Required by json4s parse function */
  implicit val formats: Formats = Serialization.formats(NoTypeHints) + SkippingKindSerializer

  /** Flint Spark index monitor */
  val flintIndexMonitor: FlintSparkIndexMonitor =
    new FlintSparkIndexMonitor(spark, flintMetadataLogService)

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
  def createIndex(index: FlintSparkIndex, ignoreIfExists: Boolean = false): Unit =
    withTransaction[Unit](index.name(), "Create Flint index", forceInit = true) { tx =>
      val indexName = index.name()
      if (flintClient.exists(indexName)) {
        if (!ignoreIfExists) {
          throw new IllegalStateException(s"Flint index $indexName already exists")
        }
      } else {
        val metadata = index.metadata()
        tx
          .initialLog(latest => latest.state == EMPTY || latest.state == DELETED)
          .transientLog(latest => latest.copy(state = CREATING))
          .finalLog(latest => latest.copy(state = ACTIVE))
          .commit(latest =>
            // TODO: update index metadata
            if (latest == null) { // in case transaction capability is disabled
              flintClient.createIndex(indexName, metadata)
            } else {
              logInfo(s"Creating index with metadata log entry ID ${latest.id}")
              flintClient.createIndex(indexName, metadata.copy(latestId = Some(latest.id)))
            })
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
  def refreshIndex(indexName: String): Option[String] =
    withTransaction[Option[String]](indexName, "Refresh Flint index") { tx =>
      val index = describeIndex(indexName)
        .getOrElse(throw new IllegalStateException(s"Index $indexName doesn't exist"))
      val indexRefresh = FlintSparkIndexRefresh.create(indexName, index)
      tx
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
      flintIndexMetadataService
        .getAllIndexMetadata(indexNamePattern)
        .asScala
        .map { case (indexName, metadata) =>
          attachLatestLogEntry(indexName, metadata)
        }
        .toList
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
      val metadata = flintIndexMetadataService.getIndexMetadata(indexName)
      val metadataWithEntry = attachLatestLogEntry(indexName, metadata)
      FlintSparkIndexFactory.create(metadataWithEntry)
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
    val indexName = index.name()
    validateUpdateAllowed(
      describeIndex(indexName)
        .getOrElse(throw new IllegalStateException(s"Index $indexName doesn't exist"))
        .options,
      index.options)

    withTransaction[Option[String]](indexName, "Update Flint index") { tx =>
      // Relies on validation to forbid auto-to-auto and manual-to-manual updates
      index.options.autoRefresh() match {
        case true => updateIndexManualToAuto(index, tx)
        case false => updateIndexAutoToManual(index, tx)
      }
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
  def deleteIndex(indexName: String): Boolean =
    withTransaction[Boolean](indexName, "Delete Flint index") { tx =>
      if (flintClient.exists(indexName)) {
        tx
          .initialLog(latest =>
            latest.state == ACTIVE || latest.state == REFRESHING || latest.state == FAILED)
          .transientLog(latest => latest.copy(state = DELETING))
          .finalLog(latest => latest.copy(state = DELETED))
          .commit(_ => {
            // TODO: share same transaction for now
            flintIndexMonitor.stopMonitor(indexName)
            stopRefreshingJob(indexName)
            true
          })
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
  def vacuumIndex(indexName: String): Boolean =
    withTransaction[Boolean](indexName, "Vacuum Flint index") { tx =>
      if (flintClient.exists(indexName)) {
        tx
          .initialLog(latest => latest.state == DELETED)
          .transientLog(latest => latest.copy(state = VACUUMING))
          .finalLog(_ => NO_LOG_ENTRY)
          .commit(_ => {
            flintClient.deleteIndex(indexName)
            // TODO: delete index metadata
            true
          })
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
  def recoverIndex(indexName: String): Boolean =
    withTransaction[Boolean](indexName, "Recover Flint index") { tx =>
      val index = describeIndex(indexName)
      if (index.exists(_.options.autoRefresh())) {
        tx
          .initialLog(latest => Set(ACTIVE, REFRESHING, FAILED).contains(latest.state))
          .transientLog(latest =>
            latest.copy(state = RECOVERING, createTime = System.currentTimeMillis()))
          .finalLog(latest => {
            flintIndexMonitor.startMonitor(indexName)
            latest.copy(state = REFRESHING)
          })
          .commit(_ => {
            FlintSparkIndexRefresh
              .create(indexName, index.get)
              .start(spark, flintSparkConf)
            true
          })
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
          tx
            .initialLog(_ => true)
            .finalLog(_ => NO_LOG_ENTRY)
            .commit(_ => { false })
        } else {
          false
        }
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
   * Attaches latest log entry to metadata if available.
   *
   * @param indexName
   *   index name
   * @param metadata
   *   base flint metadata
   * @return
   *   flint metadata with latest log entry attached if available
   */
  private def attachLatestLogEntry(indexName: String, metadata: FlintMetadata): FlintMetadata = {
    val latest = flintMetadataLogService
      .getIndexMetadataLog(indexName)
      .flatMap(_.getLatest)
    if (latest.isPresent) {
      metadata.copy(latestLogEntry = Some(latest.get()))
    } else {
      metadata
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

  private def updateIndexAutoToManual(
      index: FlintSparkIndex,
      tx: OptimisticTransaction[Option[String]]): Option[String] = {
    val indexName = index.name
    val indexLogEntry = index.latestLogEntry.get
    tx
      .initialLog(latest =>
        latest.state == REFRESHING && latest.entryVersion == indexLogEntry.entryVersion)
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

  private def updateIndexManualToAuto(
      index: FlintSparkIndex,
      tx: OptimisticTransaction[Option[String]]): Option[String] = {
    val indexName = index.name
    val indexLogEntry = index.latestLogEntry.get
    val indexRefresh = FlintSparkIndexRefresh.create(indexName, index)
    tx
      .initialLog(latest =>
        latest.state == ACTIVE && latest.entryVersion == indexLogEntry.entryVersion)
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
