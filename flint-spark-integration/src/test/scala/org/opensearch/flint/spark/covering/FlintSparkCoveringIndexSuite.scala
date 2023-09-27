/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.covering

import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import org.apache.spark.FlintSuite

class FlintSparkCoveringIndexSuite extends FlintSuite {

  test("get covering index name") {
    val index =
      new FlintSparkCoveringIndex("ci", "spark_catalog.default.test", Map("name" -> "string"))
    index.name() shouldBe "flint_spark_catalog_default_test_ci_index"
  }

  test("should fail if get index name without full table name") {
    val index = new FlintSparkCoveringIndex("ci", "test", Map("name" -> "string"))
    assertThrows[IllegalArgumentException] {
      index.name()
    }
  }

  test("should fail if no indexed column given") {
    assertThrows[IllegalArgumentException] {
      new FlintSparkCoveringIndex("ci", "default.test", Map.empty)
    }
  }
}
