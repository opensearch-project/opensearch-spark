/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import org.json4s.{Formats, NoTypeHints}
import org.json4s.native.JsonMethods._
import org.json4s.native.Serialization
import org.opensearch.flint.spark.FlintSparkIndexOptions.OptionName.{AUTO_REFRESH, CHECKPOINT_LOCATION, EXTRA_OPTIONS, INCREMENTAL_REFRESH, INDEX_SETTINGS, OptionName, OUTPUT_MODE, REFRESH_INTERVAL, SCHEDULER_MODE, WATERMARK_DELAY}
import org.opensearch.flint.spark.FlintSparkIndexOptions.validateOptionNames
import org.opensearch.flint.spark.refresh.FlintSparkIndexRefresh.SchedulerMode

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
   * The scheduler mode for the Flint index refresh.
   *
   * @return
   *   scheduler mode option value
   */
  def schedulerMode(): SchedulerMode.Value = {
    // TODO: Change default value to external once the external scheduler is enabled
    val defaultMode = "internal"
    val modeStr = getOptionValue(SCHEDULER_MODE).getOrElse(defaultMode)
    SchedulerMode.fromString(modeStr)
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

    // Add default option only when auto refresh is TRUE
    if (autoRefresh() == true) {
      if (!options.contains(SCHEDULER_MODE.toString)) {
        map += (SCHEDULER_MODE.toString -> schedulerMode().toString)
      }

      // The query will be executed in micro-batch mode using the internal scheduler
      // The default interval for the external scheduler is 15 minutes.
      if (SchedulerMode.EXTERNAL == schedulerMode() && !options.contains(
          REFRESH_INTERVAL.toString)) {
        map += (REFRESH_INTERVAL.toString -> "15 minutes")
      }
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
}
