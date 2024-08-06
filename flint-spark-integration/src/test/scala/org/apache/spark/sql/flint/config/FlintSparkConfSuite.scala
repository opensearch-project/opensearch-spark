/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.flint.config

import java.util.Optional

import scala.collection.JavaConverters._

import org.opensearch.flint.core.http.FlintRetryOptions._
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import org.apache.spark.FlintSuite
import org.apache.spark.sql.flint.config.FlintSparkConf.{MONITOR_INITIAL_DELAY_SECONDS, MONITOR_INTERVAL_SECONDS, MONITOR_MAX_ERROR_COUNT}

class FlintSparkConfSuite extends FlintSuite {
  test("test spark conf") {
    withSparkConf("spark.datasource.flint.host", "spark.datasource.flint.read.scroll_size") {
      spark.conf.set("spark.datasource.flint.host", "127.0.0.1")
      spark.conf.set("spark.datasource.flint.read.scroll_size", "10")

      val flintOptions = FlintSparkConf().flintOptions()
      assert(flintOptions.getHost == "127.0.0.1")
      assert(flintOptions.getScrollSize.get() == 10)

      // default value
      assert(flintOptions.getPort == 9200)
      assert(flintOptions.getRefreshPolicy == "false")
    }
  }

  test("test spark options") {
    val options = FlintSparkConf(Map("write.batch_size" -> "10", "write.id_name" -> "id").asJava)
    assert(options.batchSize() == 10)
    assert(options.docIdColumnName().isDefined)
    assert(options.docIdColumnName().get == "id")

    // default value
    assert(options.flintOptions().getHost == "localhost")
    assert(options.flintOptions().getPort == 9200)
  }

  test("test retry options default values") {
    val retryOptions = FlintSparkConf().flintOptions().getRetryOptions
    retryOptions.getMaxRetries shouldBe DEFAULT_MAX_RETRIES
    retryOptions.getRetryableHttpStatusCodes shouldBe DEFAULT_RETRYABLE_HTTP_STATUS_CODES
    retryOptions.getRetryableExceptionClassNames shouldBe Optional.empty
  }

  test("test specified retry options") {
    val retryOptions = FlintSparkConf(
      Map(
        "retry.max_retries" -> "5",
        "retry.http_status_codes" -> "429,502,503,504",
        "retry.exception_class_names" -> "java.net.ConnectException").asJava)
      .flintOptions()
      .getRetryOptions

    retryOptions.getMaxRetries shouldBe 5
    retryOptions.getRetryableHttpStatusCodes shouldBe "429,502,503,504"
    retryOptions.getRetryableExceptionClassNames.get() shouldBe "java.net.ConnectException"
  }

  test("test metadata access AWS credentials provider option") {
    withSparkConf("spark.metadata.accessAWSCredentialsProvider") {
      spark.conf.set(
        "spark.metadata.accessAWSCredentialsProvider",
        "com.example.MetadataAccessCredentialsProvider")
      val flintOptions = FlintSparkConf().flintOptions()
      assert(flintOptions.getCustomAwsCredentialsProvider == "")
      assert(
        flintOptions.getMetadataAccessAwsCredentialsProvider == "com.example.MetadataAccessCredentialsProvider")
    }
  }

  test("test batch bytes options") {
    val defaultConf = FlintSparkConf(Map[String, String]().asJava)
    defaultConf.batchBytes() shouldBe 1024 * 1024
    defaultConf.flintOptions().getBatchBytes shouldBe 1024 * 1024

    val overrideConf = FlintSparkConf(Map("write.batch_bytes" -> "4mb").asJava)
    overrideConf.batchBytes() shouldBe 4 * 1024 * 1024
    overrideConf.flintOptions().getBatchBytes shouldBe 4 * 1024 * 1024
  }

  test("test index monitor options") {
    val defaultConf = FlintSparkConf()
    defaultConf.monitorInitialDelaySeconds() shouldBe 15
    defaultConf.monitorIntervalSeconds() shouldBe 60
    defaultConf.monitorMaxErrorCount() shouldBe 5

    withSparkConf(MONITOR_MAX_ERROR_COUNT.key, MONITOR_INTERVAL_SECONDS.key) {
      setFlintSparkConf(MONITOR_INITIAL_DELAY_SECONDS, 5)
      setFlintSparkConf(MONITOR_INTERVAL_SECONDS, 30)
      setFlintSparkConf(MONITOR_MAX_ERROR_COUNT, 10)

      val overrideConf = FlintSparkConf()
      defaultConf.monitorInitialDelaySeconds() shouldBe 5
      overrideConf.monitorIntervalSeconds() shouldBe 30
      overrideConf.monitorMaxErrorCount() shouldBe 10
    }
  }

  /**
   * Delete index `indexNames` after calling `f`.
   */
  protected def withSparkConf(configs: String*)(f: => Unit): Unit = {
    try {
      f
    } finally {
      configs.foreach { config => spark.conf.unset(config) }
    }
  }
}
