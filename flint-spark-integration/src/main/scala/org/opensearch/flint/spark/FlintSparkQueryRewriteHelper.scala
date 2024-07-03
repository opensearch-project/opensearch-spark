/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import org.apache.spark.sql.catalyst.expressions.{And, Attribute, BinaryComparison, EqualTo, Expression, GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual, Literal, Or}

/**
 * Query rewrite helper that provides common utilities for query rewrite rule of various Flint
 * indexes.
 */
trait FlintSparkQueryRewriteHelper {

  /**
   * Determines if the given filter expression consists solely of AND operations and no OR
   * operations, implying that it's a conjunction of conditions.
   *
   * @param filter
   *   The filter expression to check.
   * @return
   *   True if the filter contains only AND operations, False if any OR operations are found.
   */
  def isConjunction(filter: Expression): Boolean = {
    filter.collectFirst { case Or(_, _) =>
      true
    }.isEmpty
  }

  /**
   * Determines if the conditions in an index filter can subsume those in a query filter. This is
   * essential to verify if all outputs that satisfy the index filter also satisfy the query
   * filter, indicating that the index can potentially optimize the query.
   *
   * @param indexFilter
   *   The filter expression defined from the index, required to be a conjunction.
   * @param queryFilter
   *   The filter expression present in the user query, required to be a conjunction.
   * @return
   *   True if the index filter can subsume the query filter, otherwise False.
   */
  def subsume(indexFilter: Expression, queryFilter: Expression): Boolean = {
    require(isConjunction(indexFilter), "Index filter is not a conjunction")
    require(isConjunction(queryFilter), "Query filter is not a conjunction")

    // Flatten a potentially nested conjunction into a sequence of individual conditions
    def flattenConditions(filter: Expression): Seq[Expression] = filter match {
      case And(left, right) => flattenConditions(left) ++ flattenConditions(right)
      case other => Seq(other)
    }
    val indexConditions = flattenConditions(indexFilter)
    val queryConditions = flattenConditions(queryFilter)

    // Ensures that every condition in the index filter is subsumed by at least one condition
    // in the query filter
    indexConditions.forall { indexCondition =>
      queryConditions.exists { queryCondition =>
        (indexCondition, queryCondition) match {
          case (
                indexComparison @ BinaryComparison(indexCol: Attribute, _),
                queryComparison @ BinaryComparison(queryCol: Attribute, _))
              if indexCol.name == queryCol.name =>
            Range(indexComparison).subsume(Range(queryComparison))
          case _ => false
        }
      }
    }
  }

  /**
   * Represents a range with optional lower and upper bounds.
   *
   * @param lower
   *   The optional lower bound
   * @param upper
   *   The optional upper bound
   */
  case class Range(lower: Option[Bound], upper: Option[Bound]) {

    /**
     * Determines if this range subsumes (completely covers) another range. A range is considered
     * to subsume another if its lower bound is less restrictive and its upper bound is more
     * restrictive than those of the other range.
     *
     * @param other
     *   The other range to compare against.
     * @return
     *   True if this range subsumes the other, otherwise false.
     */
    def subsume(other: Range): Boolean = {
      // Subsumption check helper for lower and upper bound
      def subsume(
          thisBound: Option[Bound],
          otherBound: Option[Bound],
          comp: (Bound, Bound) => Boolean): Boolean =
        (thisBound, otherBound) match {
          case (Some(a), Some(b)) => comp(a, b)
          case (None, _) => true // this is unbounded and thus can subsume any other bound
          case (_, None) => false // other is unbounded and thus cannot be subsumed by any
        }
      subsume(lower, other.lower, _.lessThanOrEqualTo(_)) &&
      subsume(upper, other.upper, _.greaterThanOrEqualTo(_))
    }
  }

  object Range {

    /**
     * Constructs a Range object from a binary comparison expression, translating comparison
     * operators into bounds with appropriate inclusivity.
     *
     * @param condition
     *   The binary comparison
     */
    def apply(condition: BinaryComparison): Range = condition match {
      case GreaterThan(_, value: Literal) =>
        Range(Some(Bound(value, inclusive = false)), None)
      case GreaterThanOrEqual(_, value: Literal) =>
        Range(Some(Bound(value, inclusive = true)), None)
      case LessThan(_, value: Literal) =>
        Range(None, Some(Bound(value, inclusive = false)))
      case LessThanOrEqual(_, value: Literal) =>
        Range(None, Some(Bound(value, inclusive = true)))
      case EqualTo(_, value: Literal) =>
        Range(Some(Bound(value, inclusive = true)), Some(Bound(value, inclusive = true)))
      case _ => Range(None, None) // For unsupported or complex conditions
    }
  }

  /**
   * Represents a bound (lower or upper) in a range, defined by a literal value and its
   * inclusiveness.
   *
   * @param value
   *   The literal value defining the bound.
   * @param inclusive
   *   Indicates whether the bound is inclusive.
   */
  case class Bound(value: Literal, inclusive: Boolean) {

    /**
     * Checks if this bound is less than or equal to another bound, considering inclusiveness.
     *
     * @param other
     *   The bound to compare against.
     * @return
     *   True if this bound is less than or equal to the other bound.
     */
    def lessThanOrEqualTo(other: Bound): Boolean = {
      val cmp = value.value.asInstanceOf[Comparable[Any]].compareTo(other.value.value)
      cmp < 0 || (cmp == 0 && (inclusive || !other.inclusive))
    }

    /**
     * Checks if this bound is greater than or equal to another bound, considering inclusiveness.
     *
     * @param other
     *   The bound to compare against.
     * @return
     *   True if this bound is greater than or equal to the other bound.
     */
    def greaterThanOrEqualTo(other: Bound): Boolean = {
      val cmp = value.value.asInstanceOf[Comparable[Any]].compareTo(other.value.value)
      cmp > 0 || (cmp == 0 && (inclusive || !other.inclusive))
    }
  }
}
