/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.covering

import java.util

import org.opensearch.flint.spark.FlintSpark
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

    /**
     * Prerequisite:
     * ```
     *   1) Not an insert statement
     *   2) Relation is supported, ex. Iceberg, Delta, File. (is this check required?)
     *   3) Any covering index on the table:
     *     3.1) doesn't have filtering condition
     *     3.2) cover all columns present in the query
     * ```
     */
    case relation @ LogicalRelation(_, _, Some(table), false)
        if !plan.isInstanceOf[V2WriteCommand] =>
      val qualifiedTableName = qualifyTableName(flint.spark, table.qualifiedName)
      val indexPattern = getFlintIndexName("*", qualifiedTableName)
      val indexes = flint.describeIndexes(indexPattern)

      // Collect all columns needed by the query except those in relation. This is because this rule
      // executes before push down optimization, relation includes all columns in the table.
      val requiredCols =
        plan
          .collect {
            case _: LogicalRelation => Set.empty[String]
            case other => other.expressions.flatMap(_.references).map(_.name).toSet
          }
          .flatten
          .toSet

      // Choose the first covering index that meets all criteria
      indexes
        .collectFirst {
          case index: FlintSparkCoveringIndex
              if index.filterCondition.isEmpty &&
                requiredCols.subsetOf(index.indexedColumns.keySet) =>
            val ds = new FlintDataSourceV2
            val options = new CaseInsensitiveStringMap(util.Map.of("path", index.name()))
            val inferredSchema = ds.inferSchema(options)
            val flintTable = ds.getTable(inferredSchema, Array.empty, options)

            // Adjust attributes to match the original plan's output
            // TODO: replace original source column type with filed type in index metadata?
            val outputAttributes =
              index.indexedColumns.keys
                .map(colName => relation.output.find(_.name == colName).get)
                .toSeq

            // Create the DataSourceV2 scan with corrected attributes
            DataSourceV2Relation(flintTable, outputAttributes, None, None, options)
        }
        .getOrElse(relation) // If no index found, return the original relation
  }
}
