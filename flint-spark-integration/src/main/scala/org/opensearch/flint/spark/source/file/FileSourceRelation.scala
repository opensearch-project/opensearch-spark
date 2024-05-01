/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.source.file

import org.opensearch.flint.spark.source.FlintSparkSourceRelation

import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.execution.datasources.LogicalRelation

/**
 * Concrete source relation implementation for Spark built-in file-based data sources.
 *
 * @param plan
 *   the `LogicalRelation` that represents the plan associated with the File-based table
 */
case class FileSourceRelation(override val plan: LogicalRelation)
    extends FlintSparkSourceRelation {

  override def tableName: String =
    plan.catalogTable.get // catalogTable must be present as pre-checked in source relation provider's
      .qualifiedName

  override def output: Seq[AttributeReference] = plan.output
}
