/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import org.mockito.Mockito.{when, RETURNS_DEEP_STUBS}
import org.opensearch.flint.spark.refresh.FlintSparkIndexRefresh
import org.opensearch.flint.spark.refresh.FlintSparkIndexRefresh.RefreshMode._
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar.mock

import org.apache.spark.FlintSuite

class FlintSparkIndexRefreshSuite extends FlintSuite with Matchers {

  /** Test index name */
  val indexName: String = "test"

  /** Mock Flint index */
  var index: FlintSparkIndex = _

  override def beforeEach(): Unit = {
    index = mock[FlintSparkIndex](RETURNS_DEEP_STUBS)
  }

  test("should auto refresh if auto refresh option enabled") {
    when(index.options.autoRefresh()).thenReturn(true)

    val refresh = FlintSparkIndexRefresh.create(indexName, index)
    refresh.refreshMode shouldBe AUTO
  }

  test("should full refresh if both auto and incremental refresh option disabled") {
    when(index.options.autoRefresh()).thenReturn(false)
    when(index.options.incrementalRefresh()).thenReturn(false)

    val refresh = FlintSparkIndexRefresh.create(indexName, index)
    refresh.refreshMode shouldBe FULL
  }

  test(
    "should incremental refresh if auto refresh disabled but incremental refresh option enabled") {
    when(index.options.autoRefresh()).thenReturn(false)
    when(index.options.incrementalRefresh()).thenReturn(true)

    val refresh = FlintSparkIndexRefresh.create(indexName, index)
    refresh.refreshMode shouldBe INCREMENTAL
  }
}
