/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import org.apache.spark.sql.catalyst.expressions.{And, Attribute, BinaryComparison, EqualTo, Expression, GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual, Literal}

/**
 * Query rewrite helper that provides common utilities for query rewrite rule of various Flint
 * indexes.
 */
trait FlintSparkQueryRewriteHelper {

  /**
   * Determines if the conditions in an index filter can subsume those in a query filter. This is
   * essential to verify if all outputs that satisfy the index filter also satisfy the query
   * filter, indicating that the index can potentially optimize the query.
   *
   * @param indexFilter
   *   The filter expression defined from the index
   * @param queryFilter
   *   The filter expression present in the user query
   * @return
   *   True if the index filter can subsume the query filter, otherwise False
   */
  def subsume(indexFilter: Expression, queryFilter: Expression): Boolean = {
    def flattenConditions(filter: Expression): Seq[Expression] = filter match {
      case And(left, right) => flattenConditions(left) ++ flattenConditions(right)
      case other => Seq(other)
    }

    val indexConditions = flattenConditions(indexFilter)
    val queryConditions = flattenConditions(queryFilter)

    // Each index condition must subsume in a query condition
    // otherwise it means index data cannot "cover" query condition
    indexConditions.forall { indexCondition =>
      queryConditions.exists { queryCondition =>
        (indexCondition, queryCondition) match {
          case (
                i @ BinaryComparison(indexCol: Attribute, _),
                q @ BinaryComparison(queryCol: Attribute, _)) if indexCol.name == queryCol.name =>
            Range(i).subsume(Range(q))
          case _ => false
        }
      }
    }
  }

  case class Bound(value: Literal, inclusive: Boolean) {

    def lessThanOrEqualTo(other: Bound): Boolean = {
      val cmp = value.value.asInstanceOf[Comparable[Any]].compareTo(other.value.value)
      cmp < 0 || (cmp == 0 && inclusive && other.inclusive)
    }
  }

  case class Range(lower: Option[Bound], upper: Option[Bound]) {

    def subsume(other: Range): Boolean = {
      val isLowerSubsumed = (lower, other.lower) match {
        case (Some(a), Some(b)) => a.lessThanOrEqualTo(b)
        case (None, _) => true // `bound1` is unbounded and thus can subsume anything
        case (_, None) => false // `bound2` is unbounded and thus cannot be subsumed
        case (None, None) => true
      }
      val isUpperSubsumed = (upper, other.upper) match {
        case (Some(a), Some(b)) => b.lessThanOrEqualTo(a)
        case (None, _) => true // `bound1` is unbounded and thus can subsume anything
        case (_, None) => false // `bound2` is unbounded and thus cannot be subsumed
        case (None, None) => true
      }
      isLowerSubsumed && isUpperSubsumed
    }
  }

  object Range {
    def apply(condition: BinaryComparison): Range = condition match {
      case GreaterThan(_, value: Literal) => Range(Some(Bound(value, inclusive = false)), None)
      case GreaterThanOrEqual(_, value: Literal) =>
        Range(Some(Bound(value, inclusive = true)), None)
      case LessThan(_, value: Literal) => Range(None, Some(Bound(value, inclusive = false)))
      case LessThanOrEqual(_, value: Literal) => Range(None, Some(Bound(value, inclusive = true)))
      case EqualTo(_, value: Literal) =>
        Range(Some(Bound(value, inclusive = true)), Some(Bound(value, inclusive = true)))
      case _ => Range(None, None) // For unsupported or complex conditions
    }
  }
}
