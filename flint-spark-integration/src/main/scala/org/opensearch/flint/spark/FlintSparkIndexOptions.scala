/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import java.util.Locale

import org.opensearch.flint.spark.FlintSparkIndexOptions.OptionName.{
  AUTO_REFRESH, CHECKPOINT_LOCATION, INDEX_SETTINGS, OptionName, REFRESH_INTERVAL
}
import org.opensearch.flint.spark.FlintSparkIndexOptions.validateOptionNames

/**
 * Flint Spark index configurable options.
 *
 * @param options
 *   index option mappings
 */
case class FlintSparkIndexOptions(options: Map[String, String]) {

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
   * The checkpoint location which maybe required by Flint index's refresh.
   *
   * @return
   *   checkpoint location path
   */
  def checkpointLocation(): Option[String] = getOptionValue(CHECKPOINT_LOCATION)

  /**
   * The index settings for OpenSearch index created.
   *
   * @return
   *   index setting JSON
   */
  def indexSettings(): Option[String] = getOptionValue(INDEX_SETTINGS)

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
    map.result()
  }

  private def getOptionValue(name: OptionName): Option[String] = {
    options.get(name.toString)
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
    val CHECKPOINT_LOCATION: OptionName.Value = Value("checkpoint_location")
    val INDEX_SETTINGS: OptionName.Value = Value("index_settings")

    /**
     * @return
     *   convert enum name to lowercase as public option name
     */
    override def toString(): String = {
      super.toString().toLowerCase(Locale.ROOT)
    }
  }

  // This method has to be here otherwise Scala compilation failure
  def validateOptionNames(options: Map[String, String]): Unit = {
    val allowedNames = OptionName.values.map(_.toString)
    val unknownNames = options.keys.filterNot(allowedNames.contains)

    require(unknownNames.isEmpty,
      s"option name ${unknownNames.mkString(",")} is invalid")
  }
}
