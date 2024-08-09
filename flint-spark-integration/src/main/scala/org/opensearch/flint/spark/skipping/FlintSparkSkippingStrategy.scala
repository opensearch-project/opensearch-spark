/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.skipping

import org.json4s.CustomSerializer
import org.json4s.JsonAST.JString
import org.opensearch.flint.spark.skipping.FlintSparkSkippingStrategy.SkippingKind.SkippingKind

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, GetStructField}
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.catalyst.util.CharVarcharCodegenUtils
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StringType

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
    val PARTITION, VALUE_SET, MIN_MAX, BLOOM_FILTER = Value
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
  case class IndexColumnExtractor(indexColName: String) {

    def unapply(expr: Expression): Option[Column] = {
      val colName = extractColumnName(expr).mkString(".")
      if (colName == indexColName) {
        Some(col(indexColName))
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
        /**
         * Since Spark 3.4 add read-side padding, char_col = "sample char" became
         * (staticinvoke(class org.apache.spark.sql.catalyst.util.CharVarcharCodegenUtils,
         * StringType, readSidePadding, char_col#47, 20, true, false, true) = sample char )
         *
         * When create skipping index, Spark did write-side padding. So read-side push down can be
         * ignored. More reading, https://issues.apache.org/jira/browse/SPARK-40697
         */
        case StaticInvoke(staticObject, StringType, "readSidePadding", arguments, _, _, _, _)
            if classOf[CharVarcharCodegenUtils].isAssignableFrom(staticObject) =>
          extractColumnName(arguments.head)
        case _ => Seq.empty
      }
    }
  }
}
