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
    SCHEDULER_MODE.toString shouldBe "scheduler_mode"
    REFRESH_INTERVAL.toString shouldBe "refresh_interval"
    INCREMENTAL_REFRESH.toString shouldBe "incremental_refresh"
    CHECKPOINT_LOCATION.toString shouldBe "checkpoint_location"
    WATERMARK_DELAY.toString shouldBe "watermark_delay"
    OUTPUT_MODE.toString shouldBe "output_mode"
    INDEX_SETTINGS.toString shouldBe "index_settings"
    ID_EXPRESSION.toString shouldBe "id_expression"
    EXTRA_OPTIONS.toString shouldBe "extra_options"
  }

  test("should return specified option value") {
    val options = FlintSparkIndexOptions(
      Map(
        "auto_refresh" -> "true",
        "scheduler_mode" -> "external",
        "refresh_interval" -> "1 Minute",
        "incremental_refresh" -> "true",
        "checkpoint_location" -> "s3://test/",
        "watermark_delay" -> "30 Seconds",
        "output_mode" -> "complete",
        "index_settings" -> """{"number_of_shards": 3}""",
        "id_expression" -> """sha1(col("timestamp"))""",
        "extra_options" ->
          """ {
            |   "alb_logs": {
            |     "opt1": "val1"
            |   },
            |   "sink": {
            |     "opt2": "val2",
            |     "opt3": "val3"
            |   }
            | }""".stripMargin))

    options.autoRefresh() shouldBe true
    options.isExternalSchedulerEnabled() shouldBe true
    options.refreshInterval() shouldBe Some("1 Minute")
    options.incrementalRefresh() shouldBe true
    options.checkpointLocation() shouldBe Some("s3://test/")
    options.watermarkDelay() shouldBe Some("30 Seconds")
    options.outputMode() shouldBe Some("complete")
    options.indexSettings() shouldBe Some("""{"number_of_shards": 3}""")
    options.idExpression() shouldBe Some("""sha1(col("timestamp"))""")
    options.extraSourceOptions("alb_logs") shouldBe Map("opt1" -> "val1")
    options.extraSinkOptions() shouldBe Map("opt2" -> "val2", "opt3" -> "val3")
  }

  test("should return extra source option value and empty sink option values") {
    val options = FlintSparkIndexOptions(
      Map("extra_options" ->
        """ {
            |   "alb_logs": {
            |     "opt1": "val1"
            |   }
            | }""".stripMargin))

    options.extraSourceOptions("alb_logs") shouldBe Map("opt1" -> "val1")
    options.extraSourceOptions("alb_logs_metrics") shouldBe empty
    options.extraSinkOptions() shouldBe empty
  }

  test("should return default option value if unspecified") {
    val options = FlintSparkIndexOptions(Map.empty)

    options.autoRefresh() shouldBe false
    options.isExternalSchedulerEnabled() shouldBe false
    options.refreshInterval() shouldBe empty
    options.checkpointLocation() shouldBe empty
    options.watermarkDelay() shouldBe empty
    options.outputMode() shouldBe empty
    options.indexSettings() shouldBe empty
    options.idExpression() shouldBe empty
    options.extraSourceOptions("alb_logs") shouldBe empty
    options.extraSinkOptions() shouldBe empty
    options.optionsWithDefault should contain("auto_refresh" -> "false")
  }

  test("should return include unspecified option if it has default value") {
    val options = FlintSparkIndexOptions(Map("refresh_interval" -> "1 Minute"))

    options.optionsWithDefault shouldBe Map(
      "auto_refresh" -> "false",
      "incremental_refresh" -> "false",
      "refresh_interval" -> "1 Minute")
  }

  test(
    "should return include default refresh_interval option with auto_refresh=true and scheduler_mode=external") {
    val options =
      FlintSparkIndexOptions(Map("auto_refresh" -> "true", "scheduler_mode" -> "external"))

    options.optionsWithDefault shouldBe Map(
      "auto_refresh" -> "true",
      "scheduler_mode" -> "external",
      "incremental_refresh" -> "false")
  }

  test("should report error if any unknown option name") {
    the[IllegalArgumentException] thrownBy
      FlintSparkIndexOptions(Map("autoRefresh" -> "true"))

    the[IllegalArgumentException] thrownBy
      FlintSparkIndexOptions(Map("AUTO_REFRESH" -> "true"))

    the[IllegalArgumentException] thrownBy {
      FlintSparkIndexOptions(Map("auto_refresh" -> "true", "indexSetting" -> "test"))
    } should have message "requirement failed: option name indexSetting is invalid"

    the[IllegalArgumentException] thrownBy {
      FlintSparkIndexOptions(Map("scheduler_mode" -> "invalid_mode"))
    } should have message "requirement failed: Scheduler mode invalid_mode is invalid. Must be 'internal' or 'external'."
  }
}
