/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.function

import org.scalatest.matchers.should.Matchers

import org.apache.spark.FlintSuite

class TumbleFunctionSuite extends FlintSuite with Matchers {

  test("should require both column name and window expression as arguments") {
    // TumbleFunction.functionBuilder(AttributeReference())
  }
}
