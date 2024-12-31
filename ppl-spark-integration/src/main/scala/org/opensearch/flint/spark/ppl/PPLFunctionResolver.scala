/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.flint.spark.ppl

import org.apache.calcite.sql.SqlOperator
import org.apache.calcite.sql.fun.SqlStdOperatorTable

case class PPLFunctionResolver() {
  def resolve(name: String): SqlOperator = {
    name match {
      case "=" => SqlStdOperatorTable.EQUALS
      case "+" => SqlStdOperatorTable.PLUS
      case "-" => SqlStdOperatorTable.MINUS
      case "avg" => SqlStdOperatorTable.AVG
      case "min" => SqlStdOperatorTable.MIN
      case "max" => SqlStdOperatorTable.MAX
      case "count" => SqlStdOperatorTable.COUNT
    }
  }
}
