/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.util

import org.opensearch.flint.spark.util.QualifiedTableName.{catalogName, tableNameWithoutCatalog}

import org.apache.spark.sql.SparkSession

/**
 * Qualified table name class that encapsulates table name parsing and qualifying utility. This is
 * useful because Spark doesn't associate catalog info in logical plan even after analyzed.
 *
 * @param tableName
 *   table name maybe qualified or not
 * @param spark
 *   Spark session to get current catalog and database info
 */
case class QualifiedTableName(spark: SparkSession, tableName: String) {

  /** Qualified table name */
  private val qualifiedTableName: String = {
    val parts = tableName.split("\\.")
    if (parts.length == 1) {
      s"$currentCatalog.$currentDatabase.$tableName"
    } else if (parts.length == 2) {
      s"$currentCatalog.$tableName"
    } else {
      tableName
    }
  }

  /**
   * @return
   *   Qualified table name
   */
  def name: String = qualifiedTableName

  /**
   * @return
   *   catalog name only
   */
  def catalog: String = catalogName(qualifiedTableName)

  /**
   * @return
   *   database and table name only
   */
  def nameWithoutCatalog: String = tableNameWithoutCatalog(qualifiedTableName)

  private def currentCatalog: String = {
    require(spark != null, "Spark session required to unqualify the given table name")

    val catalogMgr = spark.sessionState.catalogManager
    catalogMgr.currentCatalog.name()
  }

  private def currentDatabase: String = {
    require(spark != null, "Spark session required to unqualify the given table name")

    val catalogMgr = spark.sessionState.catalogManager
    catalogMgr.currentNamespace.mkString(".")
  }
}

/**
 * Utility methods for table name already qualified and thus dont' need Spark session.
 */
object QualifiedTableName {

  def catalogName(qualifiedTableName: String): String = {
    qualifiedTableName.substring(0, qualifiedTableName.indexOf("."))
  }

  def tableNameWithoutCatalog(qualifiedTableName: String): String = {
    qualifiedTableName.substring(qualifiedTableName.indexOf(".") + 1)
  }
}
