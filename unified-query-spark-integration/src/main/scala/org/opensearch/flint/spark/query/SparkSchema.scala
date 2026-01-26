/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.query

import java.util

import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory}
import org.apache.calcite.schema.{Schema, Table}
import org.apache.calcite.schema.impl.{AbstractSchema, AbstractTable}

import org.apache.spark.sql.SparkSession

/**
 * Implements Calcite's Schema interface by bridging Spark SQL catalogs and tables.
 *
 * Schema hierarchy: SparkSchema (catalog) -> SubSchema (database) -> Table
 *
 * @param spark
 *   The current SparkSession
 * @param catalogName
 *   The name of the catalog this schema represents
 */
class SparkSchema(spark: SparkSession, catalogName: String) extends AbstractSchema {

  /**
   * Returns sub-schemas (databases) within this catalog as a lazy map.
   *
   * @return
   *   Map of database names to Schema objects
   */
  override protected def getSubSchemaMap: util.Map[String, Schema] =
    new LazyMap[String, Schema](dbName => buildSubSchema(dbName))

  private def buildSubSchema(dbName: String): Schema = new AbstractSchema() {
    override def getTableMap: util.Map[String, Table] =
      new LazyMap[String, Table](tableName => buildTable(dbName, tableName))
  }

  private def buildTable(dbName: String, tableName: String): Table =
    new AbstractTable {
      override def getRowType(factory: RelDataTypeFactory): RelDataType = {
        val table = spark.table(s"$catalogName.$dbName.$tableName")
        SparkCalciteTypeConverter.toCalciteRowType(table.schema, factory)
      }
    }

  /**
   * A read-only map that computes values on demand using the provided function. This enables lazy
   * loading of sub-schemas and tables without querying the catalog upfront.
   *
   * @param valueFn
   *   function that computes the value for a given key
   * @tparam K
   *   key type
   * @tparam V
   *   value type
   */
  private class LazyMap[K, V](valueFn: K => V) extends util.AbstractMap[K, V] {
    override def get(key: Any): V = valueFn(key.asInstanceOf[K])

    override def entrySet(): util.Set[util.Map.Entry[K, V]] = util.Collections.emptySet()
  }
}
