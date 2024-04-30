/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.source.iceberg

import org.apache.iceberg.spark.source.SparkTable
import org.opensearch.flint.spark.source.{FlintSparkSourceRelation, FlintSparkSourceRelationProvider}

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, DataSourceV2ScanRelation}

/**
 * Source relation provider for Apache Iceberg-based source.
 *
 * @param name
 *   the name of the Iceberg source provider
 */
class IcebergSourceRelationProvider(override val name: String = "iceberg")
    extends FlintSparkSourceRelationProvider {

  override def isSupported(plan: LogicalPlan): Boolean = plan match {
    case DataSourceV2Relation(_: SparkTable, _, _, _, _) => true
    case DataSourceV2ScanRelation(DataSourceV2Relation(_: SparkTable, _, _, _, _), _, _, _) =>
      true
    case _ => false
  }

  override def getRelation(plan: LogicalPlan): FlintSparkSourceRelation = plan match {
    case relation @ DataSourceV2Relation(_: SparkTable, _, _, _, _) =>
      IcebergSourceRelation(relation)
    case DataSourceV2ScanRelation(
          relation @ DataSourceV2Relation(_: SparkTable, _, _, _, _),
          _,
          _,
          _) =>
      IcebergSourceRelation(relation)
  }
}
