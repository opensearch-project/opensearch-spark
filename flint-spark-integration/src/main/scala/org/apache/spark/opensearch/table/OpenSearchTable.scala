/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.opensearch.table

import scala.collection.JavaConverters._

import org.opensearch.flint.common.metadata.FlintMetadata
import org.opensearch.flint.core.FlintOptions
import org.opensearch.flint.core.metadata.FlintIndexMetadataServiceBuilder
import org.opensearch.flint.core.storage.FlintOpenSearchIndexMetadataService

import org.apache.spark.SparkConf
import org.apache.spark.sql.flint.datatype.FlintDataType
import org.apache.spark.sql.types.StructType

/**
 * Represents an OpenSearch table.
 *
 * @param tableName
 *   The name of the table.
 * @param metadata
 *   Metadata of the table.
 */
case class OpenSearchTable(tableName: String, metadata: Map[String, FlintMetadata]) {
  /*
   * FIXME. we use first index schema in multiple indices. we should merge StructType to widen type
   */
  lazy val schema: StructType = {
    metadata.values.headOption
      .map(m => FlintDataType.deserialize(FlintOpenSearchIndexMetadataService.serialize(m)))
      .getOrElse(StructType(Nil))
  }

  lazy val partitions: Array[PartitionInfo] = {
    metadata.map { case (partitionName, metadata) =>
      PartitionInfo.apply(partitionName, metadata.indexSettings.get)
    }.toArray
  }
}

object OpenSearchTable {

  /**
   * Creates an OpenSearchTable instance.
   *
   * @param tableName
   *   tableName support (1) single index name. (2) wildcard index name. (3) comma sep index name.
   * @param options
   *   The options for Flint.
   * @param conf
   *   Configurations for Spark application.
   * @return
   *   An instance of OpenSearchTable.
   */
  def apply(tableName: String, options: FlintOptions, conf: SparkConf): OpenSearchTable = {
    OpenSearchTable(
      tableName,
      FlintIndexMetadataServiceBuilder
        .build(options, conf)
        .getAllIndexMetadata(tableName.split(","): _*)
        .asScala
        .toMap)
  }
}
