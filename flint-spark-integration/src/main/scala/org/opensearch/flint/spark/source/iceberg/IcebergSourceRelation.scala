/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.source.iceberg

import org.opensearch.flint.spark.source.FlintSparkSourceRelation

import org.apache.spark.sql.catalyst.expressions.AttributeReference
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

  override def tableName: String =
    plan.table.name()

  override def output: Seq[AttributeReference] = plan.output
}
