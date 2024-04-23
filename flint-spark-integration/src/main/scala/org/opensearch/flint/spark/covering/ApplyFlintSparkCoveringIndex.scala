/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.covering

import java.util

import org.opensearch.flint.core.metadata.log.FlintMetadataLogEntry.IndexState.DELETED
import org.opensearch.flint.spark.{FlintSpark, FlintSparkIndex}
import org.opensearch.flint.spark.covering.FlintSparkCoveringIndex.getFlintIndexName

import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, V2WriteCommand}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.flint.{qualifyTableName, FlintDataSourceV2}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Flint Spark covering index apply rule that rewrites applicable query's table scan operator to
 * accelerate query by reducing data scanned significantly.
 *
 * @param flint
 *   Flint Spark API
 */
class ApplyFlintSparkCoveringIndex(flint: FlintSpark) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case relation @ LogicalRelation(_, _, Some(table), false)
        if !plan.isInstanceOf[V2WriteCommand] => // Not an insert statement
      val tableName = table.qualifiedName
      val requiredCols = collectAllColumnsInQueryPlan(plan)

      // Choose the first covering index that meets all criteria above
      findAllCoveringIndexesOnTable(tableName)
        .collectFirst {
          case index: FlintSparkCoveringIndex if isCoveringIndexApplicable(index, requiredCols) =>
            replaceTableRelationWithIndexRelation(relation, index)
        }
        .getOrElse(relation) // If no index found, return the original relation
  }

  private def collectAllColumnsInQueryPlan(plan: LogicalPlan): Set[String] = {
    // Collect all columns needed by the query, except those in relation. This is because this rule
    // executes before push down optimization and thus relation includes all columns in the table.
    plan
      .collect {
        case _: LogicalRelation => Set.empty[String]
        case other => other.expressions.flatMap(_.references).map(_.name).toSet
      }
      .flatten
      .toSet
  }

  private def findAllCoveringIndexesOnTable(tableName: String): Seq[FlintSparkIndex] = {
    val qualifiedTableName = qualifyTableName(flint.spark, tableName)
    val indexPattern = getFlintIndexName("*", qualifiedTableName)
    flint.describeIndexes(indexPattern)
  }

  private def isCoveringIndexApplicable(
      index: FlintSparkCoveringIndex,
      requiredCols: Set[String]): Boolean = {
    index.latestLogEntry.exists(_.state != DELETED) &&
    index.filterCondition.isEmpty && // TODO: support partial covering index later
    requiredCols.subsetOf(index.indexedColumns.keySet)
  }

  private def replaceTableRelationWithIndexRelation(
      relation: LogicalRelation,
      index: FlintSparkCoveringIndex): LogicalPlan = {
    // Replace with data source relation so as to avoid OpenSearch index required in catalog
    val ds = new FlintDataSourceV2
    val options = new CaseInsensitiveStringMap(util.Map.of("path", index.name()))
    val inferredSchema = ds.inferSchema(options)
    val flintTable = ds.getTable(inferredSchema, Array.empty, options)

    // Keep original output attributes only if available in covering index.
    // We have to reuse original attribute object because it's already analyzed
    // with exprId referenced by the other parts of the query plan.
    val outputAttributes =
      index.indexedColumns.keys
        .map(colName => relation.output.find(_.name == colName).get)
        .toSeq

    // Create the DataSourceV2 scan with corrected attributes
    DataSourceV2Relation(flintTable, outputAttributes, None, None, options)
  }
}
