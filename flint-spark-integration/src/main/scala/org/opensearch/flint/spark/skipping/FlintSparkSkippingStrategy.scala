/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.skipping

import org.json4s.CustomSerializer
import org.json4s.JsonAST.JString
import org.opensearch.flint.spark.skipping.FlintSparkSkippingStrategy.SkippingKind.SkippingKind

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, GetStructField}

/**
 * Skipping index strategy that defines skipping data structure building and reading logic.
 */
trait FlintSparkSkippingStrategy {

  /**
   * Skipping strategy kind.
   */
  val kind: SkippingKind

  /**
   * Indexed column name.
   */
  val columnName: String

  /**
   * Indexed column Spark SQL type.
   */
  val columnType: String

  /**
   * Skipping algorithm named parameters.
   */
  val parameters: Map[String, String] = Map.empty

  /**
   * @return
   *   output schema mapping from Flint field name to Flint field type
   */
  def outputSchema(): Map[String, String]

  /**
   * @return
   *   aggregators that generate skipping data structure
   */
  def getAggregators: Seq[Expression]

  /**
   * Rewrite a filtering condition on source table into a new predicate on index data based on
   * current skipping strategy.
   *
   * @param predicate
   *   filtering condition on source table
   * @return
   *   new filtering condition on index data or empty if index not applicable
   */
  def rewritePredicate(predicate: Expression): Option[Expression]
}

object FlintSparkSkippingStrategy {

  /**
   * Skipping kind enum class.
   */
  object SkippingKind extends Enumeration {
    type SkippingKind = Value

    // Use Value[s]Set because ValueSet already exists in Enumeration
    val PARTITION, VALUE_SET, MIN_MAX = Value
  }

  /** json4s doesn't serialize Enum by default */
  object SkippingKindSerializer
      extends CustomSerializer[SkippingKind](_ =>
        (
          { case JString(value) =>
            SkippingKind.withName(value)
          },
          { case kind: SkippingKind =>
            JString(kind.toString)
          }))

  /**
   * Extractor that match the given expression with the index expression in skipping index.
   *
   * @param indexColName
   *   indexed column name
   */
  case class IndexExpressionMatcher(indexColName: String) {

    def unapply(expr: Expression): Option[String] = {
      val colName = extractColumnName(expr).mkString(".")
      if (colName == indexColName) {
        Some(indexColName)
      } else {
        None
      }
    }

    /*
     * In Spark, after analysis, nested field "a.b.c" becomes:
     *  GetStructField(name="a",
     *     child=GetStructField(name="b",
     *             child=AttributeReference(name="c")))
     * TODO: To support any index expression, analyze index expression string
     */
    private def extractColumnName(expr: Expression): Seq[String] = {
      expr match {
        case attr: Attribute =>
          Seq(attr.name)
        case GetStructField(child, _, Some(name)) =>
          extractColumnName(child) :+ name
        case _ => Seq.empty
      }
    }
  }
}
