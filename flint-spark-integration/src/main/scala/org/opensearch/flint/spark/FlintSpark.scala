/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import scala.collection.JavaConverters._

import org.json4s.{Formats, NoTypeHints}
import org.json4s.native.Serialization
import org.opensearch.flint.common.metadata.{FlintIndexMetadataService, FlintMetadata}
import org.opensearch.flint.common.metadata.log.{FlintMetadataLogService, OptimisticTransaction}
import org.opensearch.flint.common.metadata.log.FlintMetadataLogEntry.IndexState._
import org.opensearch.flint.common.metadata.log.OptimisticTransaction.NO_LOG_ENTRY
import org.opensearch.flint.common.scheduler.AsyncQueryScheduler
import org.opensearch.flint.core.{FlintClient, FlintClientBuilder}
import org.opensearch.flint.core.metadata.FlintIndexMetadataServiceBuilder
import org.opensearch.flint.core.metadata.log.FlintMetadataLogServiceBuilder
import org.opensearch.flint.spark.FlintSparkIndex.ID_COLUMN
import org.opensearch.flint.spark.FlintSparkIndexOptions.OptionName._
import org.opensearch.flint.spark.covering.FlintSparkCoveringIndex
import org.opensearch.flint.spark.metadatacache.FlintMetadataCacheWriterBuilder
import org.opensearch.flint.spark.mv.FlintSparkMaterializedView
import org.opensearch.flint.spark.refresh.FlintSparkIndexRefresh
import org.opensearch.flint.spark.refresh.FlintSparkIndexRefresh.RefreshMode._
import org.opensearch.flint.spark.refresh.FlintSparkIndexRefresh.SchedulerMode
import org.opensearch.flint.spark.scheduler.{AsyncQuerySchedulerBuilder, FlintSparkJobExternalSchedulingService, FlintSparkJobInternalSchedulingService, FlintSparkJobSchedulingService}
import org.opensearch.flint.spark.scheduler.AsyncQuerySchedulerBuilder.AsyncQuerySchedulerAction
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
  override protected val flintClient: FlintClient =
    FlintClientBuilder.build(flintSparkConf.flintOptions())

  private val flintIndexMetadataService: FlintIndexMetadataService = {
    FlintIndexMetadataServiceBuilder.build(flintSparkConf.flintOptions())
  }

  private val flintMetadataCacheWriter = FlintMetadataCacheWriterBuilder.build(flintSparkConf)

  private val flintAsyncQueryScheduler: AsyncQueryScheduler = {
    AsyncQuerySchedulerBuilder.build(spark, flintSparkConf.flintOptions())
  }

  override protected val flintMetadataLogService: FlintMetadataLogService = {
    FlintMetadataLogServiceBuilder.build(flintSparkConf.flintOptions())
  }

  /** Required by json4s parse function */
  implicit val formats: Formats = Serialization.formats(NoTypeHints) + SkippingKindSerializer

  /** Flint Spark index monitor */
  val flintIndexMonitor: FlintSparkIndexMonitor =
    new FlintSparkIndexMonitor(spark, flintClient, flintMetadataLogService)

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
        val jobSchedulingService = FlintSparkJobSchedulingService.create(
          index,
          spark,
          flintAsyncQueryScheduler,
          flintSparkConf,
          flintIndexMonitor)
        tx
          .initialLog(latest => latest.state == EMPTY || latest.state == DELETED)
          .transientLog(latest => latest.copy(state = CREATING))
          .finalLog(latest => latest.copy(state = ACTIVE))
          .commit(latest => {
            val metadata = latest match {
              case null => // in case transaction capability is disabled
                index.metadata()
              case latestEntry =>
                logInfo(s"Creating index with metadata log entry ID ${latestEntry.id}")
                index
                  .metadata()
                  .copy(latestId = Some(latestEntry.id), latestLogEntry = Some(latest))
            }
            flintClient.createIndex(indexName, metadata)
            flintIndexMetadataService.updateIndexMetadata(indexName, metadata)
            flintMetadataCacheWriter.updateMetadataCache(indexName, metadata)
            jobSchedulingService.handleJob(index, AsyncQuerySchedulerAction.SCHEDULE)
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
      indexRefresh.refreshMode match {
        case AUTO => refreshIndexAuto(index, indexRefresh, tx)
        case FULL | INCREMENTAL => refreshIndexManual(index, indexRefresh, tx)
      }
    }.flatten

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
      getAllIndexMetadata(indexNamePattern)
        .map { case (indexName, metadata) =>
          attachLatestLogEntry(indexName, metadata)
        }
        .toList
        .flatMap(metadata => FlintSparkIndexFactory.create(spark, metadata))
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
      FlintSparkIndexFactory.create(spark, metadataWithEntry)
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
    val originalOptions = describeIndex(indexName)
      .getOrElse(throw new IllegalStateException(s"Index $indexName doesn't exist"))
      .options

    validateUpdateAllowed(originalOptions, index.options)
    withTransaction[Option[String]](indexName, "Update Flint index") { tx =>
      // Relies on validation to prevent:
      // 1. auto-to-auto updates besides scheduler_mode
      // 2. any manual-to-manual updates
      // 3. both refresh_mode and scheduler_mode updated
      (
        index.options.autoRefresh(),
        isSchedulerModeChanged(originalOptions, index.options)) match {
        case (true, true) => updateSchedulerMode(index, tx)
        case (true, false) => updateIndexManualToAuto(index, tx)
        case (false, false) => updateIndexAutoToManual(index, tx)
      }
    }.flatten
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
        val index = describeIndex(indexName)
        val jobSchedulingService = FlintSparkJobSchedulingService.create(
          index.get,
          spark,
          flintAsyncQueryScheduler,
          flintSparkConf,
          flintIndexMonitor)
        tx
          .initialLog(latest =>
            latest.state == ACTIVE || latest.state == REFRESHING || latest.state == FAILED)
          .transientLog(latest => latest.copy(state = DELETING))
          .finalLog(latest => latest.copy(state = DELETED))
          .commit(_ => {
            jobSchedulingService.handleJob(index.get, AsyncQuerySchedulerAction.UNSCHEDULE)
            true
          })
      } else {
        logInfo("Flint index to be deleted doesn't exist")
        false
      }
    }.getOrElse(false)

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
        val index = describeIndex(indexName)
        val options = index.get.options
        val jobSchedulingService = FlintSparkJobSchedulingService.create(
          index.get,
          spark,
          flintAsyncQueryScheduler,
          flintSparkConf,
          flintIndexMonitor)
        tx
          .initialLog(latest => latest.state == DELETED)
          .transientLog(latest => latest.copy(state = VACUUMING))
          .finalLog(_ => NO_LOG_ENTRY)
          .commit(_ => {
            jobSchedulingService.handleJob(index.get, AsyncQuerySchedulerAction.REMOVE)
            flintClient.deleteIndex(indexName)
            flintIndexMetadataService.deleteIndexMetadata(indexName)

            // Remove checkpoint folder if defined
            val checkpoint = options
              .checkpointLocation()
              .map(path => new FlintSparkCheckpoint(spark, path.asInstanceOf[String]))
            if (checkpoint.isDefined) {
              checkpoint.get.delete()
            }
            true
          })
      } else {
        logInfo("Flint index to vacuum doesn't exist")
        false
      }
    }.getOrElse(false)

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
        val updatedIndex = FlintSparkIndexFactory.createWithDefaultOptions(spark, index.get).get
        FlintSparkIndexRefresh
          .create(updatedIndex.name(), updatedIndex)
          .validate(spark)
        val jobSchedulingService = FlintSparkJobSchedulingService.create(
          updatedIndex,
          spark,
          flintAsyncQueryScheduler,
          flintSparkConf,
          flintIndexMonitor)
        tx
          .initialLog(latest => Set(ACTIVE, REFRESHING, FAILED).contains(latest.state))
          .transientLog(latest =>
            latest.copy(state = RECOVERING, createTime = System.currentTimeMillis()))
          .finalLog(latest => {
            latest.copy(state = jobSchedulingService.stateTransitions.finalStateForUpdate)
          })
          .commit(_ => {
            flintIndexMetadataService.updateIndexMetadata(indexName, updatedIndex.metadata())
            logInfo("Update index options complete")
            jobSchedulingService.handleJob(updatedIndex, AsyncQuerySchedulerAction.UPDATE)
            true
          })
      } else {
        logInfo("Index to be recovered is not auto refreshed")
        false
      }
    }.getOrElse(false)

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

  private def getAllIndexMetadata(indexNamePattern: String): Map[String, FlintMetadata] = {
    if (flintIndexMetadataService.supportsGetByIndexPattern) {
      flintIndexMetadataService
        .getAllIndexMetadata(indexNamePattern)
        .asScala
        .toMap
    } else {
      val indexNames = flintClient.getIndexNames(indexNamePattern).asScala.toArray
      flintIndexMetadataService
        .getAllIndexMetadata(indexNames: _*)
        .asScala
        .toMap
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
    val isAutoRefreshChanged = updatedOptions.autoRefresh() != originalOptions.autoRefresh()

    val changedOptions = updatedOptions.options.filterNot { case (k, v) =>
      originalOptions.options.get(k).contains(v)
    }.keySet

    if (changedOptions.isEmpty) {
      throw new IllegalArgumentException("No index option updated")
    }

    // Validate based on auto_refresh state and changes
    (isAutoRefreshChanged, updatedOptions.autoRefresh()) match {
      case (true, true) =>
        // Changing from manual to auto refresh
        if (updatedOptions.incrementalRefresh()) {
          throw new IllegalArgumentException(
            "Altering index to auto refresh while incremental refresh remains true")
        }

        val allowedOptions = Set(
          AUTO_REFRESH,
          INCREMENTAL_REFRESH,
          SCHEDULER_MODE,
          REFRESH_INTERVAL,
          CHECKPOINT_LOCATION,
          WATERMARK_DELAY)
        validateChangedOptions(changedOptions, allowedOptions, s"Altering index to auto refresh")
      case (true, false) =>
        val allowedOptions = if (updatedOptions.incrementalRefresh()) {
          // Changing from auto refresh to incremental refresh
          Set(
            AUTO_REFRESH,
            INCREMENTAL_REFRESH,
            REFRESH_INTERVAL,
            CHECKPOINT_LOCATION,
            WATERMARK_DELAY)
        } else {
          // Changing from auto refresh to full refresh
          Set(AUTO_REFRESH)
        }
        validateChangedOptions(
          changedOptions,
          allowedOptions,
          "Altering index to full/incremental refresh")

      case (false, true) =>
        // original refresh_mode is auto, only allow changing scheduler_mode and potentially refresh_interval
        var allowedOptions = Set(SCHEDULER_MODE)
        val schedulerMode =
          if (updatedOptions.isExternalSchedulerEnabled()) SchedulerMode.EXTERNAL
          else SchedulerMode.INTERNAL
        val contextPrefix =
          s"Altering index when auto_refresh remains true and scheduler_mode is $schedulerMode"
        if (updatedOptions.isExternalSchedulerEnabled()) {
          allowedOptions += REFRESH_INTERVAL
        }
        validateChangedOptions(changedOptions, allowedOptions, contextPrefix)

      case (false, false) =>
        // original refresh_mode is full/incremental, not allowed to change any options
        if (changedOptions.nonEmpty) {
          throw new IllegalArgumentException(
            "No options can be updated when auto_refresh remains false")
        }
    }
  }

  private def validateChangedOptions(
      changedOptions: Set[String],
      allowedOptions: Set[OptionName],
      context: String): Unit = {

    val allowedOptionStrings = allowedOptions.map(_.toString)

    if (!changedOptions.subsetOf(allowedOptionStrings)) {
      val invalidOptions = changedOptions -- allowedOptionStrings
      throw new IllegalArgumentException(
        s"$context only allows changing: $allowedOptions. Invalid options: $invalidOptions")
    }
  }

  private def isSchedulerModeChanged(
      originalOptions: FlintSparkIndexOptions,
      updatedOptions: FlintSparkIndexOptions): Boolean = {
    // Altering from manual to auto should not be interpreted as a scheduling mode change.
    if (!originalOptions.options.contains(SCHEDULER_MODE.toString)) {
      return false
    }
    updatedOptions.isExternalSchedulerEnabled() != originalOptions.isExternalSchedulerEnabled()
  }

  /**
   * Handles refresh for refresh mode AUTO, which is used exclusively by auto refresh index with
   * internal scheduler. Refresh start time and complete time aren't tracked for streaming job.
   * TODO: in future, track MicroBatchExecution time for streaming job and update as well
   */
  private def refreshIndexAuto(
      index: FlintSparkIndex,
      indexRefresh: FlintSparkIndexRefresh,
      tx: OptimisticTransaction[Option[String]]): Option[String] = {
    val indexName = index.name
    tx
      .initialLog(latest => latest.state == ACTIVE)
      .transientLog(latest =>
        latest.copy(state = REFRESHING, createTime = System.currentTimeMillis()))
      .finalLog(latest => {
        logInfo("Scheduling index state monitor")
        flintIndexMonitor.startMonitor(indexName)
        latest
      })
      .commit(_ => indexRefresh.start(spark, flintSparkConf))
  }

  /**
   * Handles refresh for refresh mode FULL and INCREMENTAL, which is used by full refresh index,
   * incremental refresh index, and auto refresh index with external scheduler. Stores refresh
   * start time and complete time.
   */
  private def refreshIndexManual(
      index: FlintSparkIndex,
      indexRefresh: FlintSparkIndexRefresh,
      tx: OptimisticTransaction[Option[String]]): Option[String] = {
    val indexName = index.name
    tx
      .initialLog(latest => latest.state == ACTIVE)
      .transientLog(latest => {
        val currentTime = System.currentTimeMillis()
        val updatedLatest = latest
          .copy(state = REFRESHING, createTime = currentTime, lastRefreshStartTime = currentTime)
        flintMetadataCacheWriter
          .updateMetadataCache(
            indexName,
            index.metadata.copy(latestLogEntry = Some(updatedLatest)))
        updatedLatest
      })
      .finalLog(latest => {
        logInfo("Updating index state to active")
        val updatedLatest =
          latest.copy(state = ACTIVE, lastRefreshCompleteTime = System.currentTimeMillis())
        flintMetadataCacheWriter
          .updateMetadataCache(
            indexName,
            index.metadata.copy(latestLogEntry = Some(updatedLatest)))
        updatedLatest
      })
      .commit(_ => indexRefresh.start(spark, flintSparkConf))
  }

  private def updateIndexAutoToManual(
      index: FlintSparkIndex,
      tx: OptimisticTransaction[Option[String]]): Option[String] = {
    val indexName = index.name
    val indexLogEntry = index.latestLogEntry.get
    val jobSchedulingService = FlintSparkJobSchedulingService.create(
      index,
      spark,
      flintAsyncQueryScheduler,
      flintSparkConf,
      flintIndexMonitor)
    tx
      .initialLog(latest =>
        // Index in external scheduler mode should be in active or refreshing state
        Set(jobSchedulingService.stateTransitions.initialStateForUnschedule).contains(
          latest.state) && latest.entryVersion == indexLogEntry.entryVersion)
      .transientLog(latest => latest.copy(state = UPDATING))
      .finalLog(latest =>
        latest.copy(state = jobSchedulingService.stateTransitions.finalStateForUnschedule))
      .commit(latest => {
        flintIndexMetadataService.updateIndexMetadata(indexName, index.metadata)
        flintMetadataCacheWriter
          .updateMetadataCache(indexName, index.metadata.copy(latestLogEntry = Some(latest)))
        logInfo("Update index options complete")
        jobSchedulingService.handleJob(index, AsyncQuerySchedulerAction.UNSCHEDULE)
        None
      })
  }

  private def updateIndexManualToAuto(
      index: FlintSparkIndex,
      tx: OptimisticTransaction[Option[String]]): Option[String] = {
    val indexName = index.name
    val indexLogEntry = index.latestLogEntry.get
    val jobSchedulingService = FlintSparkJobSchedulingService.create(
      index,
      spark,
      flintAsyncQueryScheduler,
      flintSparkConf,
      flintIndexMonitor)
    tx
      .initialLog(latest =>
        latest.state == jobSchedulingService.stateTransitions.initialStateForUpdate && latest.entryVersion == indexLogEntry.entryVersion)
      .transientLog(latest =>
        latest.copy(state = UPDATING, createTime = System.currentTimeMillis()))
      .finalLog(latest => {
        latest.copy(state = jobSchedulingService.stateTransitions.finalStateForUpdate)
      })
      .commit(latest => {
        flintIndexMetadataService.updateIndexMetadata(indexName, index.metadata)
        flintMetadataCacheWriter
          .updateMetadataCache(indexName, index.metadata.copy(latestLogEntry = Some(latest)))
        logInfo("Update index options complete")
        jobSchedulingService.handleJob(index, AsyncQuerySchedulerAction.UPDATE)
      })
  }

  private def updateSchedulerMode(
      index: FlintSparkIndex,
      tx: OptimisticTransaction[Option[String]]): Option[String] = {
    val indexName = index.name
    val indexLogEntry = index.latestLogEntry.get
    val internalSchedulingService =
      new FlintSparkJobInternalSchedulingService(spark, flintSparkConf, flintIndexMonitor)
    val externalSchedulingService =
      new FlintSparkJobExternalSchedulingService(flintAsyncQueryScheduler, flintSparkConf)

    val isExternal = index.options.isExternalSchedulerEnabled()
    val (initialState, finalState, oldService, newService) =
      if (isExternal) {
        (REFRESHING, ACTIVE, internalSchedulingService, externalSchedulingService)
      } else {
        (ACTIVE, REFRESHING, externalSchedulingService, internalSchedulingService)
      }

    tx
      .initialLog(latest =>
        latest.state == initialState && latest.entryVersion == indexLogEntry.entryVersion)
      .transientLog(latest => latest.copy(state = UPDATING))
      .finalLog(latest => latest.copy(state = finalState))
      .commit(_ => {
        flintIndexMetadataService.updateIndexMetadata(indexName, index.metadata)
        logInfo("Update index options complete")
        oldService.handleJob(index, AsyncQuerySchedulerAction.UNSCHEDULE)
        logInfo(
          s"Unscheduled refresh jobs from ${if (isExternal) "internal" else "external"} scheduler")
        newService.handleJob(index, AsyncQuerySchedulerAction.UPDATE)
      })
  }
}
