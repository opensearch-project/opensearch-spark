/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.covering

import java.util

import org.opensearch.flint.core.metadata.log.FlintMetadataLogEntry.IndexState.DELETED
import org.opensearch.flint.spark.{FlintSpark, FlintSparkIndex}
import org.opensearch.flint.spark.covering.FlintSparkCoveringIndex.getFlintIndexName

import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, V2WriteCommand}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.flint.{qualifyTableName, FlintDataSourceV2}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Flint Spark covering index apply rule that replace applicable query's table scan operator to
 * accelerate query by scanning covering index data.
 *
 * @param flint
 *   Flint Spark API
 */
class ApplyFlintSparkCoveringIndex(flint: FlintSpark) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case relation @ LogicalRelation(_, _, Some(table), false)
        if !plan.isInstanceOf[V2WriteCommand] => // TODO: make sure only intercept SELECT query
      val relationCols = collectRelationColumnsInQueryPlan(relation, plan)

      // Choose the first covering index that meets all criteria above
      findAllCoveringIndexesOnTable(table.qualifiedName)
        .sortBy(_.name())
        .collectFirst {
          case index: FlintSparkCoveringIndex if isCoveringIndexApplicable(index, relationCols) =>
            replaceTableRelationWithIndexRelation(index, relation)
        }
        .getOrElse(relation) // If no index found, return the original relation
  }

  private def collectRelationColumnsInQueryPlan(
      relation: LogicalRelation,
      plan: LogicalPlan): Set[String] = {
    /*
     * Collect all columns of the relation present in query plan, except those in relation itself.
     * Because this rule executes before push down optimization, relation includes all columns.
     */
    val relationColsById = relation.output.map(attr => (attr.exprId, attr)).toMap
    plan
      .collect {
        case _: LogicalRelation => Set.empty
        case other =>
          other.expressions
            .flatMap(_.references)
            .flatMap(ref =>
              relationColsById.get(ref.exprId)) // Ignore attribute not belong to target relation
            .map(attr => attr.name)
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
      relationCols: Set[String]): Boolean = {
    index.latestLogEntry.exists(_.state != DELETED) &&
    index.filterCondition.isEmpty && // TODO: support partial covering index later
    relationCols.subsetOf(index.indexedColumns.keySet)
  }

  private def replaceTableRelationWithIndexRelation(
      index: FlintSparkCoveringIndex,
      relation: LogicalRelation): LogicalPlan = {
    // Make use of data source relation to avoid Spark looking for OpenSearch index in catalog
    val ds = new FlintDataSourceV2
    val options = new CaseInsensitiveStringMap(util.Map.of("path", index.name()))
    val inferredSchema = ds.inferSchema(options)
    val flintTable = ds.getTable(inferredSchema, Array.empty, options)

    // Reuse original attribute's exprId because it's already analyzed and referenced
    // by the other parts of the query plan.
    val allRelationCols = relation.output.map(attr => (attr.name, attr)).toMap
    val outputAttributes =
      flintTable
        .schema()
        .map(field => {
          val relationCol = allRelationCols(field.name) // index column must exist in relation
          AttributeReference(field.name, field.dataType, field.nullable, field.metadata)(
            relationCol.exprId,
            relationCol.qualifier)
        })

    // Create the DataSourceV2 scan with corrected attributes
    DataSourceV2Relation(flintTable, outputAttributes, None, None, options)
  }
}
