/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.function

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo}
import org.apache.spark.sql.functions.window

/**
 * Tumble windowing function that groups row into fixed interval window without overlap.
 */
object TumbleFunction {

  val identifier: FunctionIdentifier = FunctionIdentifier("tumble")

  val exprInfo: ExpressionInfo = new ExpressionInfo(classOf[Column].getCanonicalName, "window")

  val functionBuilder: Seq[Expression] => Expression =
    (children: Seq[Expression]) => {
      // Delegate actual implementation to window() function
      val timeColumn = children.head
      val windowDuration = children(1)
      window(new Column(timeColumn), windowDuration.toString()).expr
    }

  /**
   * Function description to register current function to Spark extension.
   */
  val description: (FunctionIdentifier, ExpressionInfo, FunctionBuilder) =
    (identifier, exprInfo, functionBuilder)
}
