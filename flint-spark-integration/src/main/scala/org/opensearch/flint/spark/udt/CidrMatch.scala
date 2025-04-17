/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.udt

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.types._

/**
 * A custom expression to match IP with CIDR. This is defined as a scala class to identify it in
 * Catalyst Optimizer: {@link OpenSearchIpEqualConvertRule}
 * @param left
 *   left expression
 * @param right
 *   right expression
 */
case class CidrMatch(left: Expression, right: Expression)
    extends BinaryExpression
    with Predicate
    with Serializable {

  override def nullable: Boolean = false
  override def dataType: DataType = BooleanType
  override def prettyName: String = "ip_equal"

  override def toString: String = s"ip_equal($left, $right)"

  override def nullSafeEval(input1: Any, input2: Any): Any = {
    val ip = input1.asInstanceOf[String]
    val target = input2.asInstanceOf[String]
    ip == target // Just a placeholder; no actual logic needed here
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val leftEval = left.genCode(ctx)
    val rightEval = right.genCode(ctx)
    ev.copy(
      code = code"""
         |${leftEval.code}
         |${rightEval.code}
         |boolean ${ev.value};
         |try {
         |  String leftNormalized = java.net.InetAddress.getByName(${leftEval.value}.toString()).getHostAddress();
         |  String rightNormalized = java.net.InetAddress.getByName(${rightEval.value}.toString()).getHostAddress();
         |  ${ev.value} = leftNormalized.equals(rightNormalized);
         |} catch (java.net.UnknownHostException e) {
         |  throw new RuntimeException("Invalid IP address", e);
         |}
       """.stripMargin,
      isNull = FalseLiteral)
  }

  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression): CidrMatch = {
    copy(left = newLeft, right = newRight)
  }
}
