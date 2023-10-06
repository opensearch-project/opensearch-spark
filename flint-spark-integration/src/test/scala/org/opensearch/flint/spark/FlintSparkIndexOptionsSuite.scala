/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import org.scalatest.matchers.should.Matchers

import org.apache.spark.FlintSuite

class FlintSparkIndexOptionsSuite extends FlintSuite with Matchers {

  test("should return specified option value") {
    val options = FlintSparkIndexOptions(
      Map(
        "auto_refresh" -> "true",
        "refresh_interval" -> "1 Minute",
        "checkpoint_location" -> "s3://test/",
        "index_settings" -> """{"number_of_shards": 3}"""))

    options.autoRefresh() shouldBe true
    options.refreshInterval() shouldBe Some("1 Minute")
    options.checkpointLocation() shouldBe Some("s3://test/")
    options.indexSettings() shouldBe Some("""{"number_of_shards": 3}""")
  }

  test("should return default option value if unspecified") {
    val options = FlintSparkIndexOptions(Map.empty)

    options.autoRefresh() shouldBe false
    options.refreshInterval() shouldBe empty
    options.checkpointLocation() shouldBe empty
    options.indexSettings() shouldBe empty
    options.optionsWithDefault should contain("auto_refresh" -> "false")
  }

  test("should return default option value if unspecified with specified value") {
    val options = FlintSparkIndexOptions(Map("refresh_interval" -> "1 Minute"))

    options.optionsWithDefault shouldBe Map(
      "auto_refresh" -> "false",
      "refresh_interval" -> "1 Minute")
  }

  test("should report error if any unknown option name") {
    the[IllegalArgumentException] thrownBy
      FlintSparkIndexOptions(Map("autoRefresh" -> "true"))

    the[IllegalArgumentException] thrownBy {
      FlintSparkIndexOptions(Map("auto_refresh" -> "true", "indexSetting" -> "test"))
    } should have message "requirement failed: option name indexSetting is invalid"
  }
}
