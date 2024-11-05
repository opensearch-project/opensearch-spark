/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.flint.spark

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.{CollectionGenerator, CreateArray, Expression, GenericInternalRow, Inline, UnaryExpression}
import org.apache.spark.sql.types.{ArrayType, StructType}

class FlattenGenerator(override val child: Expression)
    extends Inline(child)
    with CollectionGenerator {

  override def checkInputDataTypes(): TypeCheckResult = child.dataType match {
    case st: StructType => TypeCheckResult.TypeCheckSuccess
    case _ => super.checkInputDataTypes()
  }

  override def elementSchema: StructType = child.dataType match {
    case st: StructType => st
    case _ => super.elementSchema
  }

  override protected def withNewChildInternal(newChild: Expression): FlattenGenerator = {
    newChild.dataType match {
      case ArrayType(st: StructType, _) => new FlattenGenerator(newChild)
      case st: StructType => withNewChildInternal(CreateArray(Seq(newChild), false))
      case _ =>
        throw new IllegalArgumentException(s"Unexpected input type ${newChild.dataType}")
    }
  }
}
