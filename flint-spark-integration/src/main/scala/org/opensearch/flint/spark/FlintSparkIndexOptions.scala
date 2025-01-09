/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import java.util.{Collections, UUID}

import org.json4s.{Formats, NoTypeHints}
import org.json4s.native.JsonMethods._
import org.json4s.native.Serialization
import org.opensearch.flint.spark.FlintSparkIndexOptions.OptionName.{AUTO_REFRESH, CHECKPOINT_LOCATION, EXTRA_OPTIONS, ID_EXPRESSION, INCREMENTAL_REFRESH, INDEX_SETTINGS, OptionName, OUTPUT_MODE, REFRESH_INTERVAL, SCHEDULER_MODE, WATERMARK_DELAY}
import org.opensearch.flint.spark.FlintSparkIndexOptions.validateOptionNames
import org.opensearch.flint.spark.refresh.FlintSparkIndexRefresh.SchedulerMode
import org.opensearch.flint.spark.scheduler.util.IntervalSchedulerParser

import org.apache.spark.sql.flint.config.FlintSparkConf

/**
 * Flint Spark index configurable options.
 *
 * @param options
 *   index option mappings
 */
case class FlintSparkIndexOptions(options: Map[String, String]) {

  implicit val formats: Formats = Serialization.formats(NoTypeHints)

  validateOptionNames(options)
  validateOptionSchedulerModeValue()

  /**
   * Is Flint index auto refreshed or manual refreshed.
   *
   * @return
   *   auto refresh option value
   */
  def autoRefresh(): Boolean = getOptionValue(AUTO_REFRESH).getOrElse("false").toBoolean

  /**
   * Is Flint index refresh in external scheduler mode. This only applies to auto refresh.
   *
   * @return
   *   true if external scheduler is enabled, false otherwise
   */
  def isExternalSchedulerEnabled(): Boolean = {
    getOptionValue(SCHEDULER_MODE).contains(SchedulerMode.EXTERNAL.toString)
  }

  /**
   * The refresh interval (only valid if auto refresh enabled).
   *
   * @return
   *   refresh interval expression
   */
  def refreshInterval(): Option[String] = getOptionValue(REFRESH_INTERVAL)

  /**
   * Is refresh incremental or full. This only applies to manual refresh.
   *
   * @return
   *   incremental option value
   */
  def incrementalRefresh(): Boolean =
    getOptionValue(INCREMENTAL_REFRESH).getOrElse("false").toBoolean

  /**
   * The checkpoint location which maybe required by Flint index's refresh.
   *
   * @return
   *   checkpoint location path
   */
  def checkpointLocation(): Option[String] = getOptionValue(CHECKPOINT_LOCATION)

  /**
   * How late the data can come and still be processed.
   *
   * @return
   *   watermark delay time expression
   */
  def watermarkDelay(): Option[String] = getOptionValue(WATERMARK_DELAY)

  /**
   * The output mode that describes how data will be written to streaming sink.
   * @return
   *   output mode
   */
  def outputMode(): Option[String] = getOptionValue(OUTPUT_MODE)

  /**
   * The index settings for OpenSearch index created.
   *
   * @return
   *   index setting JSON
   */
  def indexSettings(): Option[String] = getOptionValue(INDEX_SETTINGS)

  /**
   * An expression that generates unique value as index data row ID.
   *
   * @return
   *   ID expression
   */
  def idExpression(): Option[String] = getOptionValue(ID_EXPRESSION)

  /**
   * Extra streaming source options that can be simply passed to DataStreamReader or
   * Relation.options
   * @param source
   *   source name (full table name)
   * @return
   *   extra source option map or empty map if not exist
   */
  def extraSourceOptions(source: String): Map[String, String] = {
    parseExtraOptions(source)
  }

  /**
   * Extra streaming sink options that can be simply passed to DataStreamWriter.options()
   *
   * @return
   *   extra sink option map or empty map if not exist
   */
  def extraSinkOptions(): Map[String, String] = {
    parseExtraOptions("sink")
  }

  /**
   * @return
   *   all option values and fill default value if unspecified
   */
  def optionsWithDefault: Map[String, String] = {
    val map = Map.newBuilder[String, String]
    map ++= options

    if (!options.contains(AUTO_REFRESH.toString)) {
      map += (AUTO_REFRESH.toString -> autoRefresh().toString)
    }

    if (!options.contains(INCREMENTAL_REFRESH.toString)) {
      map += (INCREMENTAL_REFRESH.toString -> incrementalRefresh().toString)
    }
    map.result()
  }

  private def getOptionValue(name: OptionName): Option[String] = {
    options.get(name.toString)
  }

  private def parseExtraOptions(key: String): Map[String, String] = {
    getOptionValue(EXTRA_OPTIONS)
      .map(opt => (parse(opt) \ key).extract[Map[String, String]])
      .getOrElse(Map.empty)
  }

  private def validateOptionSchedulerModeValue(): Unit = {
    getOptionValue(SCHEDULER_MODE) match {
      case Some(modeStr) =>
        SchedulerMode.fromString(modeStr) // Will throw an exception if the mode is invalid
      case None => // no action needed if modeStr is empty
    }
  }

  def checkpointLocation(indexName: String, flintSparkConf: FlintSparkConf): Option[String] = {
    options.get(CHECKPOINT_LOCATION.toString) match {
      case Some(location) => Some(location)
      case None =>
        // Currently, deleting and recreating the flint index will enter same checkpoint dir.
        // Use a UUID to isolate checkpoint data.
        flintSparkConf.checkpointLocationRootDir.map { rootDir =>
          s"${rootDir.stripSuffix("/")}/$indexName/${UUID.randomUUID().toString}"
        }
    }
  }
}

object FlintSparkIndexOptions {

  /**
   * Empty options
   */
  val empty: FlintSparkIndexOptions = FlintSparkIndexOptions(Map.empty)

  /**
   * Option name Enum.
   */
  object OptionName extends Enumeration {
    type OptionName = Value
    val AUTO_REFRESH: OptionName.Value = Value("auto_refresh")
    val SCHEDULER_MODE: OptionName.Value = Value("scheduler_mode")
    val REFRESH_INTERVAL: OptionName.Value = Value("refresh_interval")
    val INCREMENTAL_REFRESH: OptionName.Value = Value("incremental_refresh")
    val CHECKPOINT_LOCATION: OptionName.Value = Value("checkpoint_location")
    val WATERMARK_DELAY: OptionName.Value = Value("watermark_delay")
    val OUTPUT_MODE: OptionName.Value = Value("output_mode")
    val INDEX_SETTINGS: OptionName.Value = Value("index_settings")
    val ID_EXPRESSION: OptionName.Value = Value("id_expression")
    val EXTRA_OPTIONS: OptionName.Value = Value("extra_options")
  }

  /**
   * Validate option names and throw exception if any unknown found.
   *
   * @param options
   *   options given
   */
  def validateOptionNames(options: Map[String, String]): Unit = {
    val allOptions = OptionName.values.map(_.toString)
    val invalidOptions = options.keys.filterNot(allOptions.contains)

    require(invalidOptions.isEmpty, s"option name ${invalidOptions.mkString(",")} is invalid")
  }

  /**
   * Updates the options with default values.
   *
   * @param indexName
   *   The index name string
   * @param options
   *   The original FlintSparkIndexOptions
   * @return
   *   Updated FlintSparkIndexOptions
   */
  def updateOptionsWithDefaults(
      indexName: String,
      options: FlintSparkIndexOptions): FlintSparkIndexOptions = {
    val flintSparkConf = new FlintSparkConf(Collections.emptyMap[String, String])

    val updatedOptions =
      new scala.collection.mutable.HashMap[String, String]() ++= options.options

    // Add checkpoint location if not present
    options.checkpointLocation(indexName, flintSparkConf).foreach { location =>
      updatedOptions += (CHECKPOINT_LOCATION.toString -> location)
    }

    // Update scheduler mode and refresh interval only if auto refresh is enabled
    if (!options.autoRefresh()) {
      return FlintSparkIndexOptions(updatedOptions.toMap)
    }

    val externalSchedulerEnabled = flintSparkConf.isExternalSchedulerEnabled
    val thresholdInterval =
      IntervalSchedulerParser.parse(flintSparkConf.externalSchedulerIntervalThreshold())
    val currentInterval = options.refreshInterval().map(IntervalSchedulerParser.parse)
    (
      externalSchedulerEnabled,
      currentInterval.isDefined,
      updatedOptions.get(SCHEDULER_MODE.toString)) match {
      case (true, true, None | Some("external"))
          if currentInterval.get.getInterval >= thresholdInterval.getInterval =>
        updatedOptions += (SCHEDULER_MODE.toString -> SchedulerMode.EXTERNAL.toString)
      case (true, true, Some("external"))
          if currentInterval.get.getInterval < thresholdInterval.getInterval =>
        throw new IllegalArgumentException(
          s"Input refresh_interval is ${options.refreshInterval().get}, required above the interval threshold of external scheduler: ${flintSparkConf
              .externalSchedulerIntervalThreshold()}")
      case (true, false, Some("external")) =>
        updatedOptions += (REFRESH_INTERVAL.toString -> flintSparkConf
          .externalSchedulerIntervalThreshold())
      case (true, false, None) =>
        updatedOptions += (SCHEDULER_MODE.toString -> SchedulerMode.EXTERNAL.toString)
        updatedOptions += (REFRESH_INTERVAL.toString -> flintSparkConf
          .externalSchedulerIntervalThreshold())
      case (false, _, Some("external")) =>
        throw new IllegalArgumentException(
          "spark.flint.job.externalScheduler.enabled is false but scheduler_mode is set to external")
      case _ =>
        updatedOptions += (SCHEDULER_MODE.toString -> SchedulerMode.INTERNAL.toString)
    }
    FlintSparkIndexOptions(updatedOptions.toMap)
  }
}
