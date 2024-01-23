/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.utils

import org.opensearch.flint.spark.FlintSparkOptimizer.withFlintOptimizerDisabled

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Expression, Predicate, UnaryExpression, Unevaluable}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}

/**
 * Spark expression utils.
 */
object ExpressionUtils {

  /**
   * Parse the given expression string.
   *
   * @param exprStr
   *   expression string
   * @return
   *   unresolved expression
   */
  def parseExprString(exprStr: String): Expression = {
    SparkSession.active.sessionState.sqlParser.parseExpression(exprStr)
  }

  /**
   * Resolve the given expression string with the logical relation.
   *
   * @param exprStr
   *   expression string
   * @param relation
   *   logical relation
   * @return
   *   resolved expression
   */
  def resolveExprString(exprStr: String, relation: LogicalPlan): Expression = {
    val sessionState = SparkSession.active.sessionState

    // Wrap unresolved expr with Filter and Relation operator
    val filter = Filter(PredicateWrapper(parseExprString(exprStr)), relation)

    // Disable Flint rule to avoid stackoverflow during analysis and optimization
    withFlintOptimizerDisabled {
      val analyzed = sessionState.analyzer.execute(filter)

      // Unwrap to get resolved expr
      analyzed
        .asInstanceOf[Filter]
        .condition
        .asInstanceOf[PredicateWrapper]
        .child
    }
  }

  /* Predicate wrapper to preserve the expression during optimization */
  case class PredicateWrapper(override val child: Expression)
      extends UnaryExpression
      with Unevaluable
      with Predicate {

    override protected def withNewChildInternal(newChild: Expression): Expression =
      copy(child = newChild)
  }
}
