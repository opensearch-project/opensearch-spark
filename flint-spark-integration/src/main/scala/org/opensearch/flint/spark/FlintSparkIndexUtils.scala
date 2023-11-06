/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import org.apache.spark.sql.catalyst.expressions.{Expression, Or}
import org.apache.spark.sql.functions.expr

/**
 * Flint Spark index utility methods.
 */
object FlintSparkIndexUtils {

  /**
   * Is the given Spark predicate string a conjunction
   *
   * @param condition
   *   predicate condition string
   * @return
   *   true if yes, otherwise false
   */
  def isConjunction(condition: String): Boolean = {
    isConjunction(expr(condition).expr)
  }

  /**
   * Is the given Spark predicate a conjunction
   *
   * @param condition
   *   predicate condition
   * @return
   *   true if yes, otherwise false
   */
  def isConjunction(condition: Expression): Boolean = {
    condition.collectFirst { case Or(_, _) =>
      true
    }.isEmpty
  }
}
