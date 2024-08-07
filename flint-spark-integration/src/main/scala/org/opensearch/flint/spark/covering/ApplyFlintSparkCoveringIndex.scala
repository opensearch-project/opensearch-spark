/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.covering

import java.util

import org.opensearch.flint.common.metadata.log.FlintMetadataLogEntry.IndexState.DELETED
import org.opensearch.flint.spark.{FlintSpark, FlintSparkQueryRewriteHelper}
import org.opensearch.flint.spark.covering.FlintSparkCoveringIndex.getFlintIndexName
import org.opensearch.flint.spark.source.{FlintSparkSourceRelation, FlintSparkSourceRelationProvider}

import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser.parseExpression
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, V2WriteCommand}
import org.apache.spark.sql.catalyst.rules.Rule
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
class ApplyFlintSparkCoveringIndex(flint: FlintSpark)
    extends Rule[LogicalPlan]
    with FlintSparkQueryRewriteHelper {

  /** All supported source relation providers */
  private val supportedProviders = FlintSparkSourceRelationProvider.getAllProviders(flint.spark)

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (plan.isInstanceOf[V2WriteCommand]) { // TODO: bypass any non-select plan
      plan
    } else {
      // Iterate each sub plan tree in the given plan
      plan transform {
        case filter @ Filter(condition, Relation(sourceRelation)) =>
          doApply(plan, sourceRelation, Some(condition))
            .map(newRelation => filter.copy(child = newRelation))
            .getOrElse(filter)
        case relation @ Relation(sourceRelation) =>
          doApply(plan, sourceRelation, None)
            .getOrElse(relation)
      }
    }
  }

  private def doApply(
      plan: LogicalPlan,
      relation: FlintSparkSourceRelation,
      queryFilter: Option[Expression]): Option[LogicalPlan] = {
    val relationCols = collectRelationColumnsInQueryPlan(plan, relation)

    // Choose the first covering index that meets all criteria above
    findAllCoveringIndexesOnTable(relation.tableName)
      .sortBy(_.name())
      .find(index => isCoveringIndexApplicable(index, queryFilter, relationCols))
      .map(index => replaceTableRelationWithIndexRelation(index, relation))
  }

  private object Relation {
    def unapply(subPlan: LogicalPlan): Option[FlintSparkSourceRelation] = {
      // Check if any source relation can support the plan node
      supportedProviders.collectFirst {
        case provider if provider.isSupported(subPlan) =>
          logInfo(s"Provider [${provider.name()}] can match plan ${subPlan.nodeName}")
          provider.getRelation(subPlan)
      }
    }
  }

  private def collectRelationColumnsInQueryPlan(
      plan: LogicalPlan,
      relation: FlintSparkSourceRelation): Set[String] = {
    /*
     * Collect all columns of the relation present in query plan, except those in relation itself.
     * Because this rule executes before push down optimization, relation includes all columns.
     */
    val relationColsById = relation.output.map(attr => (attr.exprId, attr)).toMap
    val relationCols =
      plan
        .collect {
          // Relation interface matches both file and Iceberg relation
          case r: MultiInstanceRelation if r.eq(relation.plan) => Set.empty
          case other =>
            other.expressions
              .flatMap(_.references)
              .flatMap(ref => {
                relationColsById.get(ref.exprId)
              }) // Ignore attribute not belong to current relation being rewritten
              .map(attr => attr.name)
        }
        .flatten
        .toSet

    if (relationCols.isEmpty) {
      // Return all if plan only has relation operator, e.g. SELECT * or all columns
      relationColsById.values.map(_.name).toSet
    } else {
      relationCols
    }
  }

  private def findAllCoveringIndexesOnTable(tableName: String): Seq[FlintSparkCoveringIndex] = {
    val qualifiedTableName = qualifyTableName(flint.spark, tableName)
    val indexPattern = getFlintIndexName("*", qualifiedTableName)
    val indexes =
      flint
        .describeIndexes(indexPattern)
        .collect { // cast to covering index and double check table name
          case index: FlintSparkCoveringIndex if index.tableName == qualifiedTableName => index
        }

    val indexNames = indexes.map(_.name()).mkString(",")
    logInfo(s"Found covering index [$indexNames] on table $qualifiedTableName")
    indexes
  }

  private def isCoveringIndexApplicable(
      index: FlintSparkCoveringIndex,
      queryFilter: Option[Expression],
      relationCols: Set[String]): Boolean = {
    val indexedCols = index.indexedColumns.keySet
    val subsumption = (index.filterCondition, queryFilter) match {
      case (None, _) => true // full index can cover any query filter
      case (Some(_), None) => false // partial index cannot cover query without filter
      case (Some(indexFilter), Some(_)) =>
        subsume(parseExpression(indexFilter), queryFilter.get)
    }
    val isApplicable =
      index.latestLogEntry.exists(_.state != DELETED) &&
        subsumption &&
        relationCols.subsetOf(indexedCols)

    logInfo(s"""
         | Is covering index ${index.name()} applicable: $isApplicable
         |   Index state: ${index.latestLogEntry.map(_.state)}
         |   Query filter: $queryFilter
         |   Index filter: ${index.filterCondition}
         |   Subsumption test: $subsumption
         |   Columns required: $relationCols
         |   Columns indexed: $indexedCols
         |""".stripMargin)
    isApplicable
  }

  private def replaceTableRelationWithIndexRelation(
      index: FlintSparkCoveringIndex,
      relation: FlintSparkSourceRelation): LogicalPlan = {
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
