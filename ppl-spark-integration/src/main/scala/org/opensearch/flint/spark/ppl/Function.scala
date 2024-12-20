/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.flint.spark.ppl

import java.util

import org.apache.calcite.sql.parser.SqlParserPos.ZERO
import org.apache.calcite.sql.{SqlCall, SqlLiteral, SqlNode, SqlOperator}

case class Function(functionName: String, sqlOperator: SqlOperator) {

  def createCall(function: SqlNode, operands: util.List[SqlNode], qualifier: SqlLiteral): SqlCall =
    sqlOperator.createCall(qualifier, ZERO, operands)
}
