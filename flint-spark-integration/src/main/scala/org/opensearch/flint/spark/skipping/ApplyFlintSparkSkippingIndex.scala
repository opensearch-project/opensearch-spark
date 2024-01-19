/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.skipping

import com.amazon.awslogsdataaccesslayer.connectors.spark.LogsTable
import org.opensearch.flint.spark.FlintSpark
import org.opensearch.flint.spark.skipping.FlintSparkSkippingIndex.{getSkippingIndexName, FILE_PATH_COLUMN, SKIPPING_INDEX_TYPE}

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.{And, Expression, Or, Predicate}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.{CatalogPlugin, Identifier}
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
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
        if hasNoDisjunction(condition) && !location.isInstanceOf[FlintSparkSkippingFileIndex] =>
      val index = flint.describeIndex(getIndexName(table))
      if (index.exists(_.kind == SKIPPING_INDEX_TYPE)) {
        val skippingIndex = index.get.asInstanceOf[FlintSparkSkippingIndex]
        val indexFilter = rewriteToIndexFilter(skippingIndex, filter)

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
    case filter @ Filter(
          condition: Predicate,
          relation @ DataSourceV2Relation(table, _, Some(catalog), Some(identifier), _))
        if hasNoDisjunction(condition) &&
          // Check if query plan already rewritten
          table.isInstanceOf[LogsTable] && !table.asInstanceOf[LogsTable].hasFileIndexScan() =>
      val index = flint.describeIndex(getIndexName(catalog, identifier))
      if (index.exists(_.kind == SKIPPING_INDEX_TYPE)) {
        val skippingIndex = index.get.asInstanceOf[FlintSparkSkippingIndex]
        val indexFilter = rewriteToIndexFilter(skippingIndex, filter)
        /*
         * Replace original LogsTable with a new one with file index scan:
         *  Filter(a=b)
         *  |- DataSourceV2Relation(A)
         *     |- LogsTable <== replaced with a new LogsTable with file index scan
         */
        if (indexFilter.isDefined) {
          val indexScan = flint.queryIndex(skippingIndex.name())
          val selectFileIndexScan =
            // Non hybrid scan
            // TODO: refactor common logic with file-based skipping index
            indexScan
              .filter(new Column(indexFilter.get))
              .select(FILE_PATH_COLUMN)

          // Construct LogsTable with file index scan
          // It will build scan operator using log file ids collected from file index scan
          val logsTable = table.asInstanceOf[LogsTable]
          val newTable = new LogsTable(
            logsTable.schema(),
            logsTable.options(),
            selectFileIndexScan,
            logsTable.processedFields())
          filter.copy(child = relation.copy(table = newTable))
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

  private def getIndexName(catalog: CatalogPlugin, identifier: Identifier): String = {
    val qualifiedTableName = s"${catalog.name}.${identifier}"
    getSkippingIndexName(qualifiedTableName)
  }

  private def hasNoDisjunction(condition: Expression): Boolean = {
    condition.collectFirst { case Or(_, _) =>
      true
    }.isEmpty
  }

  private def rewriteToIndexFilter(
      index: FlintSparkSkippingIndex,
      filter: Filter): Option[Expression] = {

    index.indexedColumns
      .flatMap(col => col.rewritePredicate(filter))
      .reduceOption(And)
  }
}
