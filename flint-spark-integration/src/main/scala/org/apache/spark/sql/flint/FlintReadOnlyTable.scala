/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.flint

import java.util

import scala.collection.JavaConverters._

import org.opensearch.flint.core.FlintClientBuilder

import org.apache.spark.opensearch.catalog.OpenSearchCatalog
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.catalog.TableCapability.{BATCH_READ, BATCH_WRITE, STREAMING_WRITE, TRUNCATE}
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

  // todo. currently, we use first index schema in multiple indices. we should merge StructType
  //  to widen type
  lazy val schema: StructType = {
    userSpecifiedSchema.getOrElse {
      FlintClientBuilder
        .build(flintSparkConf.flintOptions())
        .getAllIndexMetadata(OpenSearchCatalog.indexNames(name): _*)
        .values()
        .asScala
        .headOption
        .map(m => FlintDataType.deserialize(m.getContent))
        .getOrElse(StructType(Nil))
    }
  }

  override def capabilities(): util.Set[TableCapability] =
    util.EnumSet.of(BATCH_READ, BATCH_WRITE, TRUNCATE, STREAMING_WRITE)

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    FlintScanBuilder(name, schema, flintSparkConf)
  }
}
