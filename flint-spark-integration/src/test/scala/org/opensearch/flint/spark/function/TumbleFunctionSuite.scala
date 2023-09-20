/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.function

import org.scalatest.matchers.should.Matchers

import org.apache.spark.FlintSuite
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.functions.{col, lit}

class TumbleFunctionSuite extends FlintSuite with Matchers {

  test("should accept column name and window expression as arguments") {
    TumbleFunction.functionBuilder(Seq(col("timestamp").expr, lit("10 minutes").expr))
  }

  test("should fail if only column name provided") {
    assertThrows[IllegalArgumentException] {
      TumbleFunction.functionBuilder(Seq(col("timestamp").expr))
    }
  }

  test("should fail if argument type is wrong") {
    assertThrows[AnalysisException] {
      TumbleFunction.functionBuilder(Seq(col("timestamp").expr, lit(10).expr))
    }
  }
}
