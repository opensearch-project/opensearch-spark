/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.util.ShutdownHookManager

/**
 * Flint utility methods that rely on access to private code in Spark SQL package.
 */
package object flint {

  /**
   * Add shutdown hook to SparkContext with default priority.
   *
   * @param hook
   *   hook with the code to run during shutdown
   * @return
   *   a handle that can be used to unregister the shutdown hook.
   */
  def addShutdownHook(hook: () => Unit): AnyRef = {
    ShutdownHookManager.addShutdownHook(hook)
  }

  /**
   * Convert the given logical plan to Spark data frame.
   *
   * @param spark
   *   Spark session
   * @param logicalPlan
   *   logical plan
   * @return
   *   data frame
   */
  def logicalPlanToDataFrame(spark: SparkSession, logicalPlan: LogicalPlan): DataFrame = {
    Dataset.ofRows(spark, logicalPlan)
  }

  /**
   * Qualify a given table name.
   *
   * @param spark
   *   Spark session
   * @param tableName
   *   table name maybe qualified or not
   * @return
   *   qualified table name in catalog.database.table format
   */
  def qualifyTableName(spark: SparkSession, tableName: String): String = {
    val (catalog, ident) = parseTableName(spark, tableName)

    // Tricky that our Flint delegate catalog's name has to be spark_catalog
    // so we have to find its actual name in CatalogManager
    val catalogMgr = spark.sessionState.catalogManager
    val catalogName =
      catalogMgr
        .listCatalogs(Some("*"))
        .find(catalogMgr.catalog(_) == catalog)
        .getOrElse(catalog.name())

    s"$catalogName.${ident.namespace.mkString(".")}.${ident.name}"
  }

  /**
   * Parse a given table name into its catalog and table identifier.
   *
   * @param spark
   *   Spark session
   * @param tableName
   *   table name maybe qualified or not
   * @return
   *   Spark catalog and table identifier
   */
  def parseTableName(spark: SparkSession, tableName: String): (CatalogPlugin, Identifier) = {
    // Create a anonymous class to access CatalogAndIdentifier
    new LookupCatalog {
      override protected val catalogManager: CatalogManager = spark.sessionState.catalogManager

      def parseTableName(): (CatalogPlugin, Identifier) = {
        val parts = tableName.split("\\.").toSeq
        parts match {
          case CatalogAndIdentifier(catalog, ident) => (catalog, ident)
        }
      }
    }.parseTableName()
  }

  /**
   * Load table for the given table identifier in the catalog.
   *
   * @param catalog
   *   Spark catalog
   * @param ident
   *   table identifier
   * @return
   *   Spark table
   */
  def loadTable(catalog: CatalogPlugin, ident: Identifier): Option[Table] = {
    CatalogV2Util.loadTable(catalog, ident)
  }
}
