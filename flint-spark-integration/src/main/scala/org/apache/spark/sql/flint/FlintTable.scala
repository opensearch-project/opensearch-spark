/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.flint

import java.util

import org.apache.spark.sql.connector.catalog.SupportsWrite
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

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    FlintWriteBuilder(name, info, flintSparkConf)
  }
}
