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

class FlintSparkConfSuite extends FlintSuite {
  test("test spark conf") {
    withSparkConf("spark.datasource.flint.host", "spark.datasource.flint.read.scroll_size") {
      spark.conf.set("spark.datasource.flint.host", "127.0.0.1")
      spark.conf.set("spark.datasource.flint.read.scroll_size", "10")

      val flintOptions = FlintSparkConf().flintOptions()
      assert(flintOptions.getHost == "127.0.0.1")
      assert(flintOptions.getScrollSize == 10)

      // default value
      assert(flintOptions.getPort == 9200)
      assert(flintOptions.getRefreshPolicy == "wait_for")
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
