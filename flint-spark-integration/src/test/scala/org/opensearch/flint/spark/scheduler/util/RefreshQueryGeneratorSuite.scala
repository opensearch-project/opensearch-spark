/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.scheduler.util;

import org.mockito.Mockito._
import org.opensearch.flint.common.metadata.FlintMetadata
import org.opensearch.flint.spark.FlintSparkIndex
import org.opensearch.flint.spark.covering.FlintSparkCoveringIndex
import org.opensearch.flint.spark.mv.FlintSparkMaterializedView
import org.opensearch.flint.spark.skipping.FlintSparkSkippingIndex
import org.scalatest.matchers.should.Matchers

import org.apache.spark.SparkFunSuite

class RefreshQueryGeneratorTest extends SparkFunSuite with Matchers {

  val testTable = "dummy.default.testTable"
  val expectedTableName = "`dummy`.`default`.`testTable`"

  val mockMetadata = mock(classOf[FlintMetadata])

  test("generateRefreshQuery should return correct query for FlintSparkSkippingIndex") {
    val mockIndex = mock(classOf[FlintSparkSkippingIndex])
    when(mockIndex.metadata()).thenReturn(mockMetadata)
    when(mockIndex.tableName).thenReturn(testTable)

    val result = RefreshQueryGenerator.generateRefreshQuery(mockIndex)
    result shouldBe s"REFRESH SKIPPING INDEX ON ${expectedTableName}"
  }

  test("generateRefreshQuery should return correct query for FlintSparkCoveringIndex") {
    val mockIndex = mock(classOf[FlintSparkCoveringIndex])
    when(mockIndex.indexName).thenReturn("testIndex")
    when(mockIndex.tableName).thenReturn(testTable)

    val result = RefreshQueryGenerator.generateRefreshQuery(mockIndex)
    result shouldBe s"REFRESH INDEX testIndex ON ${expectedTableName}"
  }

  test("generateRefreshQuery should return correct query for FlintSparkMaterializedView") {
    val mockIndex = mock(classOf[FlintSparkMaterializedView])
    when(mockIndex.metadata()).thenReturn(mockMetadata)
    when(mockIndex.mvName).thenReturn(testTable)

    val result = RefreshQueryGenerator.generateRefreshQuery(mockIndex)
    result shouldBe s"REFRESH MATERIALIZED VIEW ${expectedTableName}"
  }

  test("generateRefreshQuery should throw IllegalArgumentException for unsupported index type") {
    val mockIndex = mock(classOf[FlintSparkIndex])
    when(mockIndex.metadata()).thenReturn(mockMetadata)
    when(mockIndex.metadata().source).thenReturn(testTable)

    val exception = intercept[IllegalArgumentException] {
      RefreshQueryGenerator.generateRefreshQuery(mockIndex)
    }
    exception.getMessage should include("Unsupported index type")
  }
}
