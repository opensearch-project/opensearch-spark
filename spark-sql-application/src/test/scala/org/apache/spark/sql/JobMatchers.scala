/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql

import org.scalatest.matchers.should.Matchers

trait JobMatchers extends Matchers {
  def assertEqualDataframe(expected: DataFrame, result: DataFrame): Unit = {
    assert(expected.schema === result.schema)
    assert(expected.collect() === result.collect())
  }
}
