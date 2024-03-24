/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import org.json4s.{Formats, NoTypeHints}
import org.json4s.native.JsonMethods._
import org.json4s.native.Serialization
import org.opensearch.flint.spark.FlintSparkIndexOptions.OptionName.{AUTO_REFRESH, CHECKPOINT_LOCATION, EXTRA_OPTIONS, INCREMENTAL_REFRESH, INDEX_SETTINGS, OptionName, OUTPUT_MODE, REFRESH_INTERVAL, WATERMARK_DELAY}
import org.opensearch.flint.spark.FlintSparkIndexOptions.validateOptionNames
import org.opensearch.flint.spark.refresh.FlintSparkIndexRefresh.RefreshMode._

/**
 * Flint Spark index configurable options.
 *
 * @param options
 *   index option mappings
 */
case class FlintSparkIndexOptions(options: Map[String, String]) {

  implicit val formats: Formats = Serialization.formats(NoTypeHints)

  validateOptionNames(options)

  /**
   * Is Flint index auto refreshed or manual refreshed.
   *
   * @return
   *   auto refresh option value
   */
  def autoRefresh(): Boolean = getOptionValue(AUTO_REFRESH).getOrElse("false").toBoolean

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
    if (!options.contains(INCREMENTAL_REFRESH.toString)) {
      map += (INCREMENTAL_REFRESH.toString -> incrementalRefresh().toString)
    }
    map.result()
  }

  /**
   * Merge two FlintSparkIndexOptions. If an option exists in both instances, the value from the
   * other instance overwrites the value from this instance.
   * @param other
   *   options to update
   * @return
   *   updated Flint Spark index options
   */
  def update(other: FlintSparkIndexOptions): FlintSparkIndexOptions = {
    val updatedOptions = FlintSparkIndexOptions(options ++ other.options)
    validateUpdateAllowed(other, updatedOptions)
    updatedOptions
  }

  private def getOptionValue(name: OptionName): Option[String] = {
    options.get(name.toString)
  }

  private def parseExtraOptions(key: String): Map[String, String] = {
    getOptionValue(EXTRA_OPTIONS)
      .map(opt => (parse(opt) \ key).extract[Map[String, String]])
      .getOrElse(Map.empty)
  }

  /**
   * Validate the index update options are allowed.
   * @param other
   *   options to update
   * @param updatedOptions
   *   the updated options
   */
  private def validateUpdateAllowed(
      other: FlintSparkIndexOptions,
      updatedOptions: FlintSparkIndexOptions): Unit = {
    // auto_refresh must change
    if (updatedOptions.autoRefresh() == autoRefresh()) {
      throw new IllegalArgumentException("auto_refresh option must be updated")
    }

    val refreshMode = (updatedOptions.autoRefresh(), updatedOptions.incrementalRefresh()) match {
      case (true, false) => AUTO
      case (false, false) => FULL
      case (false, true) => INCREMENTAL
      case (true, true) =>
        throw new IllegalArgumentException(
          "auto_refresh and incremental_refresh options cannot both be true")
    }

    // validate allowed options depending on refresh mode
    val allowedOptions = refreshMode match {
      case FULL => Set(AUTO_REFRESH, INCREMENTAL_REFRESH)
      case AUTO | INCREMENTAL =>
        Set(
          AUTO_REFRESH,
          INCREMENTAL_REFRESH,
          REFRESH_INTERVAL,
          CHECKPOINT_LOCATION,
          WATERMARK_DELAY)
    }
    if (!other.options.keys.forall(allowedOptions.map(_.toString).contains)) {
      throw new IllegalArgumentException(
        s"Altering index to ${refreshMode} refresh only allows options: ${allowedOptions}")
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
