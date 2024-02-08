/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.refresh

import org.opensearch.flint.spark.FlintSparkIndex
import org.opensearch.flint.spark.refresh.FlintSparkIndexRefresh.RefreshMode.{FULL, RefreshMode}

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.SaveMode.Overwrite
import org.apache.spark.sql.flint.FlintDataSourceV2.FLINT_DATASOURCE
import org.apache.spark.sql.flint.config.FlintSparkConf

/**
 * Index refresh that fully refreshes the index from the given source data frame.
 *
 * @param indexName
 *   Flint index name
 * @param index
 *   Flint index
 * @param source
 *   refresh from this data frame representing a micro batch or from the beginning
 */
class FullIndexRefresh(
    indexName: String,
    index: FlintSparkIndex,
    source: Option[DataFrame] = None)
    extends FlintSparkIndexRefresh {

  override def refreshMode: RefreshMode = FULL

  override def start(spark: SparkSession, flintSparkConf: FlintSparkConf): Option[String] = {
    logInfo(s"Start refreshing index $indexName in full mode")
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
