/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.skipping

import org.opensearch.flint.spark.FlintSpark
import org.opensearch.flint.spark.FlintSparkIndexUtils.isConjunction
import org.opensearch.flint.spark.skipping.FlintSparkSkippingIndex.{getSkippingIndexName, SKIPPING_INDEX_TYPE}

import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.{And, Expression, Predicate}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.flint.qualifyTableName

/**
 * Flint Spark skipping index apply rule that rewrites applicable query's filtering condition and
 * table scan operator to leverage additional skipping data structure and accelerate query by
 * reducing data scanned significantly.
 *
 * @param flint
 *   Flint Spark API
 */
class ApplyFlintSparkSkippingIndex(flint: FlintSpark) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case filter @ Filter( // TODO: abstract pattern match logic for different table support
          condition: Predicate,
          relation @ LogicalRelation(
            baseRelation @ HadoopFsRelation(location, _, _, _, _, _),
            _,
            Some(table),
            false))
        if isConjunction(condition) && !location.isInstanceOf[FlintSparkSkippingFileIndex] =>
      val index = flint.describeIndex(getIndexName(table))
      if (index.exists(_.kind == SKIPPING_INDEX_TYPE)) {
        val skippingIndex = index.get.asInstanceOf[FlintSparkSkippingIndex]
        val indexFilter = rewriteToIndexFilter(skippingIndex, condition)

        /*
         * Replace original file index with Flint skipping file index:
         *  Filter(a=b)
         *  |- LogicalRelation(A)
         *     |- HadoopFsRelation
         *        |- FileIndex <== replaced with FlintSkippingFileIndex
         */
        if (indexFilter.isDefined) {
          val indexScan = flint.queryIndex(skippingIndex.name())
          val fileIndex = FlintSparkSkippingFileIndex(location, indexScan, indexFilter.get)
          val indexRelation = baseRelation.copy(location = fileIndex)(baseRelation.sparkSession)
          filter.copy(child = relation.copy(relation = indexRelation))
        } else {
          filter
        }
      } else {
        filter
      }
  }

  private def getIndexName(table: CatalogTable): String = {
    // Because Spark qualified name only contains database.table without catalog
    // the limitation here is qualifyTableName always use current catalog.
    val tableName = table.qualifiedName
    val qualifiedTableName = qualifyTableName(flint.spark, tableName)
    getSkippingIndexName(qualifiedTableName)
  }

  private def rewriteToIndexFilter(
      index: FlintSparkSkippingIndex,
      condition: Expression): Option[Expression] = {

    def tryEachStrategy(expr: Expression): Option[Expression] =
      index.indexedColumns.flatMap(_.rewritePredicate(expr)).headOption

    condition match {
      case and: And =>
        // Rewrite left and right expression recursively
        and.children.flatMap(child => rewriteToIndexFilter(index, child)).reduceOption(And)
      case expr => tryEachStrategy(expr)
    }
  }
}
