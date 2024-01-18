/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.refresh

import org.opensearch.flint.spark.FlintSparkIndex

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.SaveMode.Overwrite
import org.apache.spark.sql.flint.FlintDataSourceV2.FLINT_DATASOURCE
import org.apache.spark.sql.flint.config.FlintSparkConf

/**
 * Index refresher that manually and fully refreshes the index from the given source.
 *
 * @param indexName
 *   Flint index name
 * @param index
 *   Flint index
 * @param source
 *   refresh from this data frame representing a micro batch or from the beginning
 */
class FullManualIndexRefresher(
    indexName: String,
    index: FlintSparkIndex,
    source: Option[DataFrame] = None)
    extends FlintSparkIndexRefresher {

  override def start(spark: SparkSession, flintSparkConf: FlintSparkConf): Option[String] = {
    logInfo(s"Start refreshing index $indexName in full-manual mode")
    index
      .build(spark, source)
      .write
      .format(FLINT_DATASOURCE)
      .options(flintSparkConf.properties)
      .mode(Overwrite)
      .save(indexName)
    None
  }
}
