/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.flint

import org.opensearch.flint.spark.skipping.bloomfilter.BloomFilterMightContain

import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.sql.flint.config.FlintSparkConf
import org.apache.spark.sql.types.StructType

case class FlintScan(
    tableName: String,
    schema: StructType,
    options: FlintSparkConf,
    pushedPredicates: Array[Predicate])
    extends Scan
    with Batch {

  override def readSchema(): StructType = schema

  override def planInputPartitions(): Array[InputPartition] = {
    Array(OpenSearchInputPartition())
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    FlintPartitionReaderFactory(tableName, schema, options, pushedPredicates)
  }

  override def toBatch: Batch = this

  /**
   * Print pushedPredicates when explain(mode="extended"). Learn from SPARK JDBCScan.
   */
  override def description(): String = {
    super.description() + ", PushedPredicates: " + pushedPredicates
      .map {
        case p if p.name().equalsIgnoreCase(BloomFilterMightContain.NAME) => p.name()
        case p => p.toString()
      }
      .mkString("[", ", ", "]")
  }
}

// todo. add partition support.
private[spark] case class OpenSearchInputPartition() extends InputPartition {}
