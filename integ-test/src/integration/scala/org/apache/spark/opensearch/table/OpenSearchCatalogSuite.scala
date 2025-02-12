/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.opensearch.table

import org.opensearch.flint.spark.FlintSparkSuite

trait OpenSearchCatalogSuite extends FlintSparkSuite {
  override lazy val catalogName = "dev"

  override def beforeAll(): Unit = {
    super.beforeAll()

    spark.conf.set(
      s"spark.sql.catalog.${catalogName}",
      "org.apache.spark.opensearch.catalog.OpenSearchCatalog")
    spark.conf.set(s"spark.sql.catalog.${catalogName}.opensearch.port", s"$openSearchPort")
    spark.conf.set(s"spark.sql.catalog.${catalogName}.opensearch.host", openSearchHost)
    spark.conf.set(
      s"spark.sql.catalog.${catalogName}.opensearch.write.refresh_policy",
      "wait_for")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
  }
}
