/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.source.file

import org.opensearch.flint.spark.source.{FlintSparkSourceRelation, FlintSparkSourceRelationProvider}

import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.LogicalRelation

/**
 * Concrete source relation implementation for Spark built-in file-based data sources.
 *
 * @param plan
 *   the relation plan associated with the file-based data source
 */
case class FileSourceRelation(override val plan: LogicalRelation)
    extends FlintSparkSourceRelation {

  override def tableName: String =
    plan.catalogTable
      .getOrElse(throw new IllegalArgumentException("No table found in the source relation plan"))
      .qualifiedName

  override def output: Seq[AttributeReference] = plan.output
}

class FileSourceRelationProvider extends FlintSparkSourceRelationProvider {

  override def isSupported(plan: LogicalPlan): Boolean = plan match {
    case LogicalRelation(_, _, Some(_), false) => true
    case _ => false
  }

  override def getRelation(plan: LogicalPlan): FlintSparkSourceRelation = {
    FileSourceRelation(plan.asInstanceOf[LogicalRelation])
  }
}
