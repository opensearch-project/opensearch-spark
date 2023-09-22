/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.mv

import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import org.apache.spark.FlintSuite

class FlintSparkMaterializedViewSuite extends FlintSuite {

  test("get mv name") {
    val mv = FlintSparkMaterializedView("default.mv", "SELECT 1", Map.empty)
    mv.name() shouldBe "flint_default_mv"
  }

  test("should fail if not full mv name") {
    val mv = FlintSparkMaterializedView("mv", "SELECT 1", Map.empty)
    assertThrows[IllegalArgumentException] {
      mv.name()
    }
  }
}
