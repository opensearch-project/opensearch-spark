/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import org.opensearch.flint.spark.udt.{IPAddress, IPAddressUDT}

import org.apache.spark.opensearch.catalog.OpenSearchCatalog
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{And, ApplyFunctionExpression, EqualTo, Expression, Literal, Not, Or, ScalaUDF}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.functions.{BoundFunction, ScalarFunction, UnboundFunction}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types.{DataType, DataTypes, IntegerType, StringType, StructType}
import org.apache.spark.unsafe.types.UTF8String

/**
 * Bounded function which compare ip field with String value containing ip/cidr string.
 */
case object IpCompareBound extends ScalarFunction[Integer] {
  override def inputTypes(): Array[DataType] = Array(IPAddressUDT, DataTypes.StringType)

  override def resultType(): DataType = DataTypes.IntegerType

  override def name(): String = "ip_compare"

  override def produceResult(input: InternalRow): Integer = {
    val ip = IPAddress(input.get(0, IPAddressUDT).asInstanceOf[UTF8String].toString)
    val value = input.get(1, StringType).asInstanceOf[UTF8String].toString
    if (ip == null || value == null) {
      null
    } else {
      ip.normalized.compareTo(value)
    }
  }
}

/**
 * Catalyst Optimizer rule for converting cidrmatch function to ip_compare for predicate pushdown.
 * This conversion is required since Spark cannot handle UDF returning boolean in predicate
 * pushdown logic. This is a workaround to convert to a EqualTo predicate. The converted predicate
 * will be pushed down to OpenSearch query by {@link FlintQueryCompiler}.
 */
object OpenSearchCidrMatchConvertRule extends Rule[LogicalPlan] {
  val CidrmatchUDFs = Set(
    "cidrmatch", // SQL
    "cidr" // PPL (converted from cidrmatch in AstExpressionBuilder)
  )

  override def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case Filter(condition: Expression, relation: DataSourceV2Relation)
        if relation.catalog.get.isInstanceOf[OpenSearchCatalog] =>
      Filter(convertCidrMatch(condition), relation)
  }

  protected def convertCidrMatch(e: Expression): Expression = {
    e match {
      case udf: ScalaUDF if CidrmatchUDFs.contains(udf.udfName.get.toLowerCase()) =>
        // converts to (ip_compare(left, right) = 0)
        val left = udf.children.head
        val right = udf.children.last
        EqualTo(
          ApplyFunctionExpression(IpCompareBound, Seq(left, right)),
          Literal(0, IntegerType))
      case And(left, right) =>
        convertAsNeeded(e, And, left, right)
      case Or(left, right) =>
        convertAsNeeded(e, Or, left, right)
      case Not(child) =>
        convertAsNeeded(e, Not, child)
      case _ => e
    }
  }

  private def convertAsNeeded(
      e: Expression,
      constructor: (Expression, Expression) => Expression,
      left: Expression,
      right: Expression) = {
    val newLeft = convertCidrMatch(left)
    val newRight = convertCidrMatch(right)
    if (newLeft == left && newRight == right) {
      e
    } else {
      constructor(newLeft, newRight)
    }
  }

  private def convertAsNeeded(
      e: Expression,
      constructor: (Expression) => Expression,
      child: Expression) = {
    val newChild = convertCidrMatch(child)
    if (newChild == child) {
      e
    } else {
      constructor(newChild)
    }
  }
}
