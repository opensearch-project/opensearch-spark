/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import org.opensearch.flint.spark.ppl.FlintSparkPPLParser

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, ExprId, UnsafeProjection}
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan}
import org.apache.spark.sql.execution.{LeafExecNode, SparkPlan, SparkStrategy}
import org.apache.spark.sql.types.StringType
import org.apache.spark.unsafe.types.UTF8String

// Logical plan to represent printing of a literal
case class PrintLiteralCommandDescriptionLogicalPlan(text: String)
    extends LogicalPlan
    with LeafNode {
  // Create a consistent AttributeReference
  override def output: Seq[Attribute] = Seq(
    AttributeReference("output_text", StringType, nullable = false)(ExprId.apply(1)))
}

// Physical plan to print the literal
case class PrintLiteralExec(text: String) extends SparkPlan with LeafExecNode {
  // Use the provided attribute as the output
  override def output: Seq[Attribute] = Seq(
    AttributeReference("output_text", StringType, nullable = false)(ExprId.apply(1)))

  override protected def doExecute(): RDD[InternalRow] = {
    // Create a row with the text as a UTF8String
    val row = InternalRow(UTF8String.fromString(text))

    // Create a projection to convert the row to UnsafeRow
    val projection = UnsafeProjection.create(output, output)
    val unsafeRow = projection(row)

    // Return an RDD with the single UnsafeRow
    sparkContext.parallelize(Seq(unsafeRow))
  }
}

// Custom strategy to handle PrintLiteralLogicalPlan
object PrintLiteralStrategy extends SparkStrategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case PrintLiteralCommandDescriptionLogicalPlan(text) =>
      PrintLiteralExec(text) :: Nil
    case _ => Nil
  }
}

/**
 * Flint PPL Spark extension entrypoint.
 */
class FlintPPLSparkExtensions extends (SparkSessionExtensions => Unit) {

  override def apply(extensions: SparkSessionExtensions): Unit = {
    // Inject custom parser (Flint-specific parser)
    extensions.injectParser { (spark, parser) =>
      new FlintSparkPPLParser(parser)
    }

    // Inject custom strategy to handle help command
    extensions.injectPlannerStrategy { sparkSession =>
      PrintLiteralStrategy
    }
  }
}
