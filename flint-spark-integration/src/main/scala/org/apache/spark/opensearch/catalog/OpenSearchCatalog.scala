/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.opensearch.catalog

import org.apache.spark.internal.Logging
import org.apache.spark.opensearch.catalog.OpenSearchCatalog.OPENSEARCH_PREFIX
import org.apache.spark.sql.catalyst.analysis.{NoSuchNamespaceException, NoSuchTableException}
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.flint.FlintReadOnlyTable
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * A Spark TableCatalog implementation wrap OpenSearch domain as Catalog.
 *
 * <p> Configuration parameters for OpenSearchCatalog:
 *
 * <ul> <li><code>opensearch.port</code>: Default is 9200.</li>
 * <li><code>opensearch.scheme</code>: Default is http. Valid values are [http, https].</li>
 * <li><code>opensearch.auth</code>: Default is noauth. Valid values are [noauth, sigv4,
 * basic].</li> <li><code>opensearch.auth.username</code>: Basic auth username.</li>
 * <li><code>opensearch.auth.password</code>: Basic auth password.</li>
 * <li><code>opensearch.region</code>: Default is us-west-2. Only used when auth is sigv4.</li>
 * </ul>
 */
class OpenSearchCatalog extends CatalogPlugin with TableCatalog with Logging {

  private var catalogName: String = _
  private var options: CaseInsensitiveStringMap = _

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    this.catalogName = name
    this.options = options
  }

  override def name(): String = catalogName

  @throws[NoSuchNamespaceException]
  override def listTables(namespace: Array[String]): Array[Identifier] = {
    throw new UnsupportedOperationException("OpenSearchCatalog does not support listTables")
  }

  @throws[NoSuchTableException]
  override def loadTable(ident: Identifier): Table = {
    logInfo(s"Loading table ${ident.name()}")
    if (!ident.namespace().exists(n => OpenSearchCatalog.isDefaultNamespace(n))) {
      throw new NoSuchTableException(ident.namespace().mkString("."), ident.name())
    }

    val conf = new java.util.HashMap[String, String](
      removePrefixFromMap(options.asCaseSensitiveMap(), OPENSEARCH_PREFIX))
    conf.put("path", ident.name())

    new FlintReadOnlyTable(conf, Option.empty)
  }

  override def createTable(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: java.util.Map[String, String]): Table = {
    throw new UnsupportedOperationException("OpenSearchCatalog does not support createTable")
  }

  override def alterTable(ident: Identifier, changes: TableChange*): Table = {
    throw new UnsupportedOperationException("OpenSearchCatalog does not support alterTable")
  }

  override def dropTable(ident: Identifier): Boolean = {
    throw new UnsupportedOperationException("OpenSearchCatalog does not support dropTable")
  }

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = {
    throw new UnsupportedOperationException("OpenSearchCatalog does not support renameTable")
  }

  private def removePrefixFromMap(
      map: java.util.Map[String, String],
      prefix: String): java.util.Map[String, String] = {
    val result = new java.util.HashMap[String, String]()
    map.forEach { (key, value) =>
      if (key.startsWith(prefix)) {
        val newKey = key.substring(prefix.length)
        result.put(newKey, value)
      } else {
        result.put(key, value)
      }
    }
    result
  }
}

object OpenSearchCatalog {

  /**
   * The reserved namespace.
   */
  val RESERVED_DEFAULT_NAMESPACE: String = "default"

  /**
   * The prefix for OpenSearch-related configuration keys.
   */
  val OPENSEARCH_PREFIX: String = "opensearch."

  /**
   * Checks if the given namespace is the reserved default namespace.
   *
   * @param namespace
   *   The namespace to check.
   * @return
   *   True if the namespace is the reserved default namespace, false otherwise.
   */
  def isDefaultNamespace(namespace: String): Boolean = {
    RESERVED_DEFAULT_NAMESPACE.equalsIgnoreCase(namespace)
  }

  /**
   * Splits the table name into index names.
   *
   * @param tableName
   *   The name of the table, potentially containing comma-separated index names.
   * @return
   *   An array of index names.
   */
  def indexNames(tableName: String): Array[String] = {
    tableName.split(",")
  }
}
