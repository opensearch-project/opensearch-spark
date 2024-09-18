/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, ExprId}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan, Project}
import org.apache.spark.sql.execution.datasources.LogicalRelation

/**
 * general utility functions for ppl to spark transformation test
 */
trait LogicalPlanTestUtils {

  /**
   * utility function to compare two logical plans while ignoring the auto-generated expressionId
   * associated with the alias which is used for projection or aggregation
   * @param plan
   * @return
   */
  def compareByString(plan: LogicalPlan): String = {
    // Create a rule to replace Alias's ExprId with a dummy id
    val rule: PartialFunction[LogicalPlan, LogicalPlan] = {
      case p: Project =>
        val newProjections = p.projectList.map {
          case alias: Alias =>
            Alias(alias.child, alias.name)(exprId = ExprId(0), qualifier = alias.qualifier)
          case other => other
        }
        p.copy(projectList = newProjections)

      case agg: Aggregate =>
        val newGrouping = agg.groupingExpressions.map {
          case alias: Alias =>
            Alias(alias.child, alias.name)(exprId = ExprId(0), qualifier = alias.qualifier)
          case other => other
        }
        val newAggregations = agg.aggregateExpressions.map {
          case alias: Alias =>
            Alias(alias.child, alias.name)(exprId = ExprId(0), qualifier = alias.qualifier)
          case other => other
        }
        agg.copy(groupingExpressions = newGrouping, aggregateExpressions = newAggregations)

      case l @ LogicalRelation(_, output, _, _) =>
        // Because the exprIds of Output attributes in LogicalRelation cannot be normalized
        // by PlanTest.normalizePlan(). We normalize it manually.
        val newOutput = output.map { a: AttributeReference =>
          AttributeReference(a.name, a.dataType, a.nullable, a.metadata)(
            exprId = ExprId(0),
            qualifier = a.qualifier)
        }
        l.copy(output = newOutput)
      case other => other
    }

    // Apply the rule using transform
    val transformedPlan = plan.transform(rule)

    // Return the string representation of the transformed plan
    transformedPlan.toString
  }
}
