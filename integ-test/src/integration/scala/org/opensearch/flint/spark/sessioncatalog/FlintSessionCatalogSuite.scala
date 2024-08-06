/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.sessioncatalog

import org.opensearch.flint.spark.FlintSparkSuite

import org.apache.spark.SparkConf

/**
 * Test with FlintDelegatingSessionCatalog.
 */
trait FlintSessionCatalogSuite extends FlintSparkSuite {
  // Override catalog name
  override lazy protected val catalogName: String = "mycatalog"

  override protected def sparkConf: SparkConf = {
    val conf = super.sparkConf
      .set("spark.sql.catalog.mycatalog", "org.opensearch.sql.FlintDelegatingSessionCatalog")
      .set("spark.sql.defaultCatalog", catalogName)
    conf
  }
}
