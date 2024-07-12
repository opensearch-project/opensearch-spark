/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.opensearch.table

import scala.collection.JavaConverters._

import org.opensearch.flint.core.{FlintClientBuilder, FlintOptions}
import org.opensearch.flint.core.metadata.FlintMetadata

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
      .map(m => FlintDataType.deserialize(m.getContent))
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
   * @return
   *   An instance of OpenSearchTable.
   */
  def apply(tableName: String, options: FlintOptions): OpenSearchTable = {
    OpenSearchTable(
      tableName,
      FlintClientBuilder
        .build(options)
        .getAllIndexMetadata(tableName.split(","): _*)
        .asScala
        .toMap)
  }
}
