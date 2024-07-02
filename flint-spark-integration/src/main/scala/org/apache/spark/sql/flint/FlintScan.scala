/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.flint

import org.apache.spark.opensearch.table.{OpenSearchTable, ShardInfo}
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.sql.flint.config.FlintSparkConf
import org.apache.spark.sql.types.StructType

case class FlintScan(
    table: OpenSearchTable,
    schema: StructType,
    options: FlintSparkConf,
    pushedPredicates: Array[Predicate])
    extends Scan
    with Batch {

  override def readSchema(): StructType = schema

  override def planInputPartitions(): Array[InputPartition] = {
    table.partitions.flatMap(p => p.shards.map(s => OpenSearchSplit(s))).toArray
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    FlintPartitionReaderFactory(schema, options, pushedPredicates)
  }

  override def toBatch: Batch = this

  /**
   * Print pushedPredicates when explain(mode="extended"). Learn from SPARK JDBCScan.
   */
  override def description(): String = {
    super.description() + ", PushedPredicates: " + seqToString(pushedPredicates)
  }

  private def seqToString(seq: Seq[Any]): String = seq.mkString("[", ", ", "]")
}

/**
 * Each OpenSearchSplit is backed by an OpenSearch shard.
 *
 * @param shardInfo
 *   shardInfo
 */
private[spark] case class OpenSearchSplit(shardInfo: ShardInfo) extends InputPartition {}
