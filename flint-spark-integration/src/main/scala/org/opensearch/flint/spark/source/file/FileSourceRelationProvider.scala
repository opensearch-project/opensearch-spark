/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.source.file

import org.opensearch.flint.spark.source.{FlintSparkSourceRelation, FlintSparkSourceRelationProvider}

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.LogicalRelation

/**
 * Source relation provider for Spark built-in file-based source.
 *
 * @param name
 *   the name of the file source provider
 */
class FileSourceRelationProvider(override val name: String = "file")
    extends FlintSparkSourceRelationProvider {

  override def isSupported(plan: LogicalPlan): Boolean = plan match {
    case LogicalRelation(_, _, Some(_), false) => true
    case _ => false
  }

  override def getRelation(plan: LogicalPlan): FlintSparkSourceRelation = {
    FileSourceRelation(plan.asInstanceOf[LogicalRelation])
  }
}
