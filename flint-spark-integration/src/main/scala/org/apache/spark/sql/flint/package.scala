/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql

import java.util.concurrent.ScheduledExecutorService

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.internal.SQLConf.DEFAULT_CATALOG
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.util.{ShutdownHookManager, ThreadUtils}

/**
 * Flint utility methods that rely on access to private code in Spark SQL package.
 */
package object flint {

  /**
   * Create daemon thread pool with the given thread group name and size.
   *
   * @param threadNamePrefix
   *   thread group name
   * @param numThreads
   *   thread pool size
   * @return
   *   thread pool executor
   */
  def newDaemonThreadPoolScheduledExecutor(
      threadNamePrefix: String,
      numThreads: Int): ScheduledExecutorService = {
    ThreadUtils.newDaemonThreadPoolScheduledExecutor(threadNamePrefix, numThreads)
  }

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

    // more reading at https://github.com/opensearch-project/opensearch-spark/issues/319.
    val catalogName = resolveCatalogName(spark, catalog)

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

  /**
   * Find field with the given name under root field recursively.
   *
   * @param rootField
   *   root field struct
   * @param fieldName
   *   field name to search
   * @return
   */
  def findField(rootField: StructType, fieldName: String): Option[StructField] = {
    rootField.findNestedField(fieldName.split('.')).map(_._2)
  }

  /**
   * Resolve catalog name. spark.sql.defaultCatalog name is returned if catalog.name is
   * spark_catalog otherwise, catalog.name is returned.
   * @see
   *   <a href="https://github.com/opensearch-project/opensearch-spark/issues/319#issuecomment
   * -2099630984">issue319</a>
   *
   * @param spark
   *   Spark Session
   * @param catalog
   *   Spark Catalog
   * @return
   *   catalog name.
   */
  def resolveCatalogName(spark: SparkSession, catalog: CatalogPlugin): String = {

    /**
     * Check if the provided catalog is a session catalog.
     */
    if (CatalogV2Util.isSessionCatalog(catalog)) {
      val defaultCatalog = spark.conf.get(DEFAULT_CATALOG)
      if (spark.sessionState.catalogManager.isCatalogRegistered(defaultCatalog)) {
        defaultCatalog
      } else {

        /**
         * It may happen when spark.sql.defaultCatalog is configured, but there's no
         * implementation. For instance, spark.sql.defaultCatalog = "unknown"
         */
        throw new IllegalStateException(s"Unknown catalog name: $defaultCatalog")
      }
    } else {
      // Return the name for non-session catalogs
      catalog.name()
    }
  }
}
