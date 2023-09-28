/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql

import org.apache.spark.sql.connector.catalog._

package object flint {

  def qualifyTableName(spark: SparkSession, tableName: String): String = {
    val (catalog, ident) = parseTableName(spark, tableName)
    s"${catalog.name}.${ident.namespace.mkString(".")}.${ident.name}"
  }

  def parseTableName(spark: SparkSession, tableName: String): (CatalogPlugin, Identifier) = {
    new LookupCatalog {
      override protected val catalogManager: CatalogManager = spark.sessionState.catalogManager

      def parseTableName(): (CatalogPlugin, Identifier) = {
        val parts = tableName.split("\\.").toSeq
        parts match {
          case CatalogAndIdentifier(catalog, ident) => (catalog, ident)
          // case _ => None
        }
      }
    }.parseTableName()
  }

  def loadTable(catalog: CatalogPlugin, ident: Identifier): Option[Table] = {
    CatalogV2Util.loadTable(catalog, ident)
  }
}
