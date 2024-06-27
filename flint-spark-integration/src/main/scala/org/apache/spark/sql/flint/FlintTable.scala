/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.flint

import java.util

import org.apache.spark.sql.connector.catalog.{SupportsWrite, TableCapability}
import org.apache.spark.sql.connector.catalog.TableCapability.{BATCH_READ, BATCH_WRITE, STREAMING_WRITE, TRUNCATE}
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.types.StructType

/**
 * FlintTable.
 * @param conf
 *   configuration
 * @param userSpecifiedSchema
 *   userSpecifiedSchema
 */
case class FlintTable(
    override val conf: util.Map[String, String],
    override val userSpecifiedSchema: Option[StructType])
    extends FlintReadOnlyTable(conf, userSpecifiedSchema)
    with SupportsWrite {

  override def capabilities(): util.Set[TableCapability] =
    util.EnumSet.of(BATCH_READ, BATCH_WRITE, TRUNCATE, STREAMING_WRITE)

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    FlintWriteBuilder(name, info, flintSparkConf)
  }
}
