/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.opensearch.table

import org.json4s.{Formats, NoTypeHints}
import org.json4s.jackson.JsonMethods
import org.json4s.native.Serialization

/**
 * Represents information about a partition in OpenSearch. Partition is backed by OpenSearch
 * Index. Each partition contain a list of Shards
 *
 * @param partitionName
 *   partition name.
 * @param shards
 *   shards.
 */
case class PartitionInfo(partitionName: String, shards: Array[ShardInfo]) {}

object PartitionInfo {
  implicit val formats: Formats = Serialization.formats(NoTypeHints)

  /**
   * Creates a PartitionInfo instance.
   *
   * @param partitionName
   *   The name of the partition.
   * @param settings
   *   The settings of the partition.
   * @return
   *   An instance of PartitionInfo.
   */
  def apply(partitionName: String, settings: String): PartitionInfo = {
    val shards =
      Range.apply(0, numberOfShards(settings)).map(id => ShardInfo(partitionName, id)).toArray
    PartitionInfo(partitionName, shards)
  }

  /**
   * Extracts the number of shards from the settings string.
   *
   * @param settingStr
   *   The settings string.
   * @return
   *   The number of shards.
   */
  def numberOfShards(settingStr: String): Int = {
    val setting = JsonMethods.parse(settingStr)
    (setting \ "index.number_of_shards").extract[String].toInt
  }
}
