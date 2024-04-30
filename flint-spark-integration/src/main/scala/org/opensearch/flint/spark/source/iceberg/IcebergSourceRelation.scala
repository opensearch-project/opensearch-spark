/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.source.iceberg

import org.apache.iceberg.spark.source.SparkTable
import org.opensearch.flint.spark.source.{FlintSparkSourceRelation, FlintSparkSourceRelationProvider}

import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation

/**
 * Concrete implementation of `FlintSparkSourceRelation` for Iceberg-based data sources. This
 * class encapsulates the handling of relations backed by Iceberg tables, which are built on top
 * of Spark's DataSourceV2 and TableProvider interfaces.
 *
 * @param plan
 *   the `DataSourceV2Relation` that represents the plan associated with the Iceberg table.
 */
case class IcebergSourceRelation(override val plan: DataSourceV2Relation)
    extends FlintSparkSourceRelation {

  /**
   * Retrieves the fully qualified name of the table from the Iceberg table metadata. If the
   * Iceberg table is not correctly referenced or the metadata is missing, an exception is thrown.
   */
  override def tableName: String =
    plan.table.name() // TODO: confirm

  /**
   * Provides the output attributes of the logical plan. These attributes represent the schema of
   * the Iceberg table as it appears in Spark's logical plan and are used to define the structure
   * of the data returned by scans of the Iceberg table.
   */
  override def output: Seq[AttributeReference] = plan.output
}

class IcebergSourceRelationProvider extends FlintSparkSourceRelationProvider {

  override def isSupported(plan: LogicalPlan): Boolean = plan match {
    case DataSourceV2Relation(_: SparkTable, _, _, _, _) => true
    case _ => false
  }

  override def getRelation(plan: LogicalPlan): FlintSparkSourceRelation = {
    IcebergSourceRelation(plan.asInstanceOf[DataSourceV2Relation])
  }
}
