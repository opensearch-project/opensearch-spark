/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.flint

import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.sql.flint.config.FlintSparkConf
import org.apache.spark.sql.types.StructType

case class FlintScan(
    tables: Seq[org.opensearch.flint.table.Table],
    schema: StructType,
    options: FlintSparkConf,
    pushedPredicates: Array[Predicate])
    extends Scan
    with Batch
    with Logging {

  override def readSchema(): StructType = schema

  override def planInputPartitions(): Array[InputPartition] = {
    tables
      .flatMap(table => {
        if (table.isSplittable()) {
          table.slice().map(table => OpenSearchSplit(table))
        } else {
          Seq(OpenSearchSplit(table))
        }
      })
      .toArray
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
 * Each OpenSearchSplit is backed by an OpenSearch index table.
 *
 * @param table
 *   {@link org.opensearch.flint.table.Table}
 */
private[spark] case class OpenSearchSplit(table: org.opensearch.flint.table.Table)
    extends InputPartition {}
