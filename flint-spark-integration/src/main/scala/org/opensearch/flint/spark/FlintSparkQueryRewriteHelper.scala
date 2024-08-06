/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import org.apache.spark.sql.catalyst.expressions._

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
    if (!isConjunction(indexFilter) || !isConjunction(queryFilter)) {
      return false
    }

    // Flatten a potentially nested conjunction into a sequence of individual conditions
    def flattenConditions(filter: Expression): Seq[Expression] = filter match {
      case And(left, right) => flattenConditions(left) ++ flattenConditions(right)
      case other => Seq(other)
    }
    val indexConditions = flattenConditions(indexFilter)
    val queryConditions = flattenConditions(queryFilter)

    // Ensures that every condition in the index filter is subsumed by at least one condition
    // on the same column in the query filter
    indexConditions.forall { indexCond =>
      queryConditions.exists { queryCond =>
        (indexCond, queryCond) match {
          case (
                indexComp @ BinaryComparison(indexCol: Attribute, _),
                queryComp @ BinaryComparison(queryCol: Attribute, _))
              if indexCol.name == queryCol.name =>
            Range(indexComp).subsume(Range(queryComp))
          case _ => false // consider as not subsumed for unsupported expression
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
  private case class Range(lower: Option[Bound], upper: Option[Bound]) {

    /**
     * Determines if this range subsumes (completely covers) another range.
     *
     * @param other
     *   The other range to compare against.
     * @return
     *   True if this range subsumes the other, otherwise false.
     */
    def subsume(other: Range): Boolean = {
      // Unknown range cannot subsume or be subsumed by any
      if (this == Range.UNKNOWN || other == Range.UNKNOWN) {
        return false
      }

      // Subsumption check helper for lower and upper bound
      def subsumeHelper(
          thisBound: Option[Bound],
          otherBound: Option[Bound],
          comp: (Bound, Bound) => Boolean): Boolean =
        (thisBound, otherBound) match {
          case (Some(a), Some(b)) => comp(a, b)
          case (None, _) => true // this is unbounded and thus can subsume any other bound
          case (_, None) => false // other is unbounded and thus cannot be subsumed by any
        }
      subsumeHelper(lower, other.lower, _.lessThanOrEqualTo(_)) &&
      subsumeHelper(upper, other.upper, _.greaterThanOrEqualTo(_))
    }
  }

  private object Range {

    /** Unknown range for unsupported binary comparison expression */
    private val UNKNOWN: Range = Range(None, None)

    /**
     * Constructs a Range object from a binary comparison expression, translating comparison
     * operators into bounds with appropriate inclusiveness.
     *
     * @param condition
     *   The binary comparison
     */
    def apply(condition: BinaryComparison): Range = condition match {
      case GreaterThan(_, Literal(value: Comparable[Any], _)) =>
        Range(Some(Bound(value, inclusive = false)), None)
      case GreaterThanOrEqual(_, Literal(value: Comparable[Any], _)) =>
        Range(Some(Bound(value, inclusive = true)), None)
      case LessThan(_, Literal(value: Comparable[Any], _)) =>
        Range(None, Some(Bound(value, inclusive = false)))
      case LessThanOrEqual(_, Literal(value: Comparable[Any], _)) =>
        Range(None, Some(Bound(value, inclusive = true)))
      case EqualTo(_, Literal(value: Comparable[Any], _)) =>
        Range(Some(Bound(value, inclusive = true)), Some(Bound(value, inclusive = true)))
      case _ => UNKNOWN
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
  private case class Bound(value: Comparable[Any], inclusive: Boolean) {

    /**
     * Checks if this bound is less than or equal to another bound, considering inclusiveness.
     *
     * @param other
     *   The bound to compare against.
     * @return
     *   True if this bound is less than or equal to the other bound, either because value is
     *   smaller, this bound is inclusive, or both bound are exclusive.
     */
    def lessThanOrEqualTo(other: Bound): Boolean = {
      val cmp = value.compareTo(other.value)
      cmp < 0 || (cmp == 0 && (inclusive || !other.inclusive))
    }

    /**
     * Checks if this bound is greater than or equal to another bound, considering inclusiveness.
     *
     * @param other
     *   The bound to compare against.
     * @return
     *   True if this bound is greater than or equal to the other bound, either because value is
     *   greater, this bound is inclusive, or both bound are exclusive.
     */
    def greaterThanOrEqualTo(other: Bound): Boolean = {
      val cmp = value.compareTo(other.value)
      cmp > 0 || (cmp == 0 && (inclusive || !other.inclusive))
    }
  }
}
