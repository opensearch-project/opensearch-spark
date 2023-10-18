/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import org.opensearch.flint.spark.FlintSparkIndexOptions.OptionName._
import org.scalatest.matchers.should.Matchers

import org.apache.spark.FlintSuite

class FlintSparkIndexOptionsSuite extends FlintSuite with Matchers {

  test("should return lowercase name as option name") {
    AUTO_REFRESH.toString shouldBe "auto_refresh"
    REFRESH_INTERVAL.toString shouldBe "refresh_interval"
    CHECKPOINT_LOCATION.toString shouldBe "checkpoint_location"
    WATERMARK_DELAY.toString shouldBe "watermark_delay"
    OUTPUT_MODE.toString shouldBe "output_mode"
    INDEX_SETTINGS.toString shouldBe "index_settings"
    EXTRA_OPTIONS.toString shouldBe "extra_options"
  }

  test("should return specified option value") {
    val options = FlintSparkIndexOptions(
      Map(
        "auto_refresh" -> "true",
        "refresh_interval" -> "1 Minute",
        "checkpoint_location" -> "s3://test/",
        "watermark_delay" -> "30 Seconds",
        "output_mode" -> "complete",
        "index_settings" -> """{"number_of_shards": 3}""",
        "extra_options" -> """{"sink": {"opt1": "val1", "opt2": "val2"}}"""))

    options.autoRefresh() shouldBe true
    options.refreshInterval() shouldBe Some("1 Minute")
    options.checkpointLocation() shouldBe Some("s3://test/")
    options.watermarkDelay() shouldBe Some("30 Seconds")
    options.outputMode() shouldBe Some("complete")
    options.indexSettings() shouldBe Some("""{"number_of_shards": 3}""")
    options.extraSinkOptions() shouldBe Map("opt1" -> "val1", "opt2" -> "val2")
  }

  test("should return default option value if unspecified") {
    val options = FlintSparkIndexOptions(Map.empty)

    options.autoRefresh() shouldBe false
    options.refreshInterval() shouldBe empty
    options.checkpointLocation() shouldBe empty
    options.watermarkDelay() shouldBe empty
    options.outputMode() shouldBe empty
    options.indexSettings() shouldBe empty
    options.extraSinkOptions() shouldBe empty
    options.optionsWithDefault should contain("auto_refresh" -> "false")
  }

  test("should return include unspecified option if it has default value") {
    val options = FlintSparkIndexOptions(Map("refresh_interval" -> "1 Minute"))

    options.optionsWithDefault shouldBe Map(
      "auto_refresh" -> "false",
      "refresh_interval" -> "1 Minute")
  }

  test("should report error if any unknown option name") {
    the[IllegalArgumentException] thrownBy
      FlintSparkIndexOptions(Map("autoRefresh" -> "true"))

    the[IllegalArgumentException] thrownBy
      FlintSparkIndexOptions(Map("AUTO_REFRESH" -> "true"))

    the[IllegalArgumentException] thrownBy {
      FlintSparkIndexOptions(Map("auto_refresh" -> "true", "indexSetting" -> "test"))
    } should have message "requirement failed: option name indexSetting is invalid"
  }
}
