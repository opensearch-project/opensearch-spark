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
 * Schema hierarchy mirrors Spark's catalog structure:
 * {{{
 *   SparkSchema (catalog)
 *       +-- SubSchema (database) - dynamic, no existence validation
 *               +-- Table
 * }}}
 *
 * @param spark
 *   The current SparkSession
 * @param catalogName
 *   The name of the catalog this schema represents
 */
class SparkSchema(spark: SparkSession, catalogName: String) extends AbstractSchema {

  override protected def getSubSchemaMap: util.Map[String, Schema] =
    new LazyMap[String, Schema](dbName => buildSubSchema(dbName))

  private def buildSubSchema(dbName: String): Schema = new AbstractSchema() {
    override def getTableMap: util.Map[String, Table] =
      new LazyMap[String, Table](tableName => buildCalciteTable(dbName, tableName))
  }

  private def buildCalciteTable(dbName: String, tableName: String): Table = {
    val sparkTable = spark.table(s"$catalogName.$dbName.$tableName")
    new AbstractTable {
      override def getRowType(typeFactory: RelDataTypeFactory): RelDataType = {
        sparkTable.schema.toCalcite(typeFactory)
      }
    }
  }

  /** A read-only map that computes values on demand using the provided function. */
  private class LazyMap[K, V](valueFn: K => V) extends util.AbstractMap[K, V] {
    override def get(key: Any): V = valueFn(key.asInstanceOf[K])

    // Returns empty set as iteration is not supported and Calcite only uses get() lookups
    override def entrySet(): util.Set[util.Map.Entry[K, V]] = util.Collections.emptySet()
  }
}
