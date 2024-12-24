/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.covering

import org.scalatest.matchers.must.Matchers.contain
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import org.apache.spark.FlintSuite
import org.apache.spark.sql.AnalysisException

class FlintSparkCoveringIndexSuite extends FlintSuite {

  test("get covering index name") {
    val index =
      new FlintSparkCoveringIndex("ci", "spark_catalog.default.test", Map("name" -> "string"))
    index.name() shouldBe "flint_spark_catalog_default_test_ci_index"
  }

  test("get covering index name on table and index name with dots") {
    val testTableDots = "spark_catalog.default.test.2023.10"
    val index = new FlintSparkCoveringIndex("ci.01", testTableDots, Map("name" -> "string"))
    index.name() shouldBe "flint_spark_catalog_default_test.2023.10_ci.01_index"
  }

  test("should fail if get index name without full table name") {
    val index = new FlintSparkCoveringIndex("ci", "test", Map("name" -> "string"))
    assertThrows[IllegalArgumentException] {
      index.name()
    }
  }

  test("can parse identifier name with special characters during index build") {
    val testTableSpecial = "spark_catalog.de-fault.test/2023/10"
    val index = new FlintSparkCoveringIndex("ci", testTableSpecial, Map("name" -> "string"))

    val error = intercept[AnalysisException] {
      index.build(spark, None)
    }
    // Getting this error means that parsing doesn't fail with unquoted identifier
    assert(error.getMessage().contains("UnresolvedRelation"))
  }

  test("should fail if no indexed column given") {
    assertThrows[IllegalArgumentException] {
      new FlintSparkCoveringIndex("ci", "default.test", Map.empty)
    }
  }
}
