/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.util

import org.scalatest.matchers.should.Matchers

import org.apache.spark.SparkFunSuite

class CleanerFactoryTest extends SparkFunSuite with Matchers {

  test("CleanerFactory should return NoOpCleaner when streaming is true") {
    val cleaner = CleanerFactory.cleaner(streaming = true)
    cleaner shouldBe NoOpCleaner
  }

  test("CleanerFactory should return ShuffleCleaner when streaming is false") {
    val cleaner = CleanerFactory.cleaner(streaming = false)
    cleaner shouldBe ShuffleCleaner
  }
}
