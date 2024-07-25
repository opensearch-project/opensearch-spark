/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.flint

import java.util

import org.opensearch.flint.table.OpenSearchCluster

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.catalog.TableCapability.BATCH_READ
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.flint.config.FlintSparkConf
import org.apache.spark.sql.flint.datatype.FlintDataType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * FlintReadOnlyTable.
 *
 * @param conf
 *   configuration
 * @param userSpecifiedSchema
 *   userSpecifiedSchema
 */
class FlintReadOnlyTable(
    val conf: util.Map[String, String],
    val userSpecifiedSchema: Option[StructType])
    extends Table
    with SupportsRead {

  lazy val sparkSession = SparkSession.active

  lazy val flintSparkConf: FlintSparkConf = FlintSparkConf(conf)

  lazy val name: String = flintSparkConf.tableName()

  lazy val tables: Seq[org.opensearch.flint.table.Table] =
    OpenSearchCluster.apply(name, flintSparkConf.flintOptions())

  lazy val resolvedTablesSchema: StructType = tables.headOption
    .map(tbl => FlintDataType.deserialize(tbl.schema().asJson()))
    .getOrElse(StructType(Nil))

  lazy val schema: StructType = {
    userSpecifiedSchema.getOrElse { resolvedTablesSchema }
  }

  override def capabilities(): util.Set[TableCapability] =
    util.EnumSet.of(BATCH_READ)

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    FlintScanBuilder(tables, schema, flintSparkConf)
  }
}
