/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql

import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.connector.catalog.{CatalogManager, CatalogPlugin}
import org.apache.spark.sql.flint.resolveCatalogName
import org.apache.spark.sql.internal.{SessionState, SQLConf}
import org.apache.spark.sql.internal.SQLConf.DEFAULT_CATALOG

class FlintCatalogSuit extends SparkFunSuite with MockitoSugar {

  test("resolveCatalogName returns default catalog name for session catalog") {
    assertCatalog()
      .withCatalogName("spark_catalog")
      .withDefaultCatalog("glue")
      .registerCatalog("glue")
      .shouldResolveCatalogName("glue")
  }

  test("resolveCatalogName returns default catalog name for spark_catalog") {
    assertCatalog()
      .withCatalogName("spark_catalog")
      .withDefaultCatalog("spark_catalog")
      .registerCatalog("spark_catalog")
      .shouldResolveCatalogName("spark_catalog")
  }

  test("resolveCatalogName should return catalog name for non-session catalogs") {
    assertCatalog()
      .withCatalogName("custom_catalog")
      .withDefaultCatalog("custom_catalog")
      .registerCatalog("custom_catalog")
      .shouldResolveCatalogName("custom_catalog")
  }

  test(
    "resolveCatalogName should throw RuntimeException when default catalog is not registered") {
    assertCatalog()
      .withCatalogName("spark_catalog")
      .withDefaultCatalog("glue")
      .registerCatalog("unknown")
      .shouldThrowException()
  }

  private def assertCatalog(): AssertionHelper = {
    new AssertionHelper
  }

  private class AssertionHelper() {
    private val spark = mock[SparkSession]
    private val catalog = mock[CatalogPlugin]
    private val sessionState = mock[SessionState]
    private val catalogManager = mock[CatalogManager]

    def withCatalogName(catalogName: String): AssertionHelper = {
      when(catalog.name()).thenReturn(catalogName)
      this
    }

    def withDefaultCatalog(catalogName: String): AssertionHelper = {
      val conf = new SQLConf
      conf.setConf(DEFAULT_CATALOG, catalogName)
      when(spark.conf).thenReturn(new RuntimeConfig(conf))
      this
    }

    def registerCatalog(catalogName: String): AssertionHelper = {
      when(spark.sessionState).thenReturn(sessionState)
      when(sessionState.catalogManager).thenReturn(catalogManager)
      when(catalogManager.isCatalogRegistered(catalogName)).thenReturn(true)
      this
    }

    def shouldResolveCatalogName(expectedCatalogName: String): Unit = {
      assert(resolveCatalogName(spark, catalog) == expectedCatalogName)
    }

    def shouldThrowException(): Unit = {
      assertThrows[RuntimeException] {
        resolveCatalogName(spark, catalog)
      }
    }
  }
}
