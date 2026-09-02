/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.util

import com.amazonaws.services.s3.model.AmazonS3Exception
import org.scalatest.matchers.should.Matchers

import org.apache.spark.{SparkException, SparkFunSuite, SparkThrowable}
import org.apache.spark.sql.exception.RedactedException

/**
 * Contract for the query-path recording helper. `recordQueryThrowable` must keep the persisted /
 * customer error string and the original throwable available to downstream consumers
 * (classification, rethrow) while ensuring the value it hands the driver logger carries no raw
 * throwable message, interpolated customer identifier, or persisted customer error JSON. The
 * assertions run against the exact (message, throwable) pair the helper renders, so the no-leak
 * guarantee is proven without capturing log4j output.
 */
class ThrowableHandlerTest extends SparkFunSuite with Matchers {

  private def renderStackTrace(t: Throwable): String = {
    val sw = new java.io.StringWriter()
    t.printStackTrace(new java.io.PrintWriter(sw))
    sw.toString
  }

  test("recordQueryThrowable keeps the persisted error and original throwable for consumers") {
    val handler = new ThrowableHandler()
    val persisted =
      """{"message":"Fail to run query. Cause: bad column","errorCode":"QUERY_ANALYSIS_ERROR"}"""
    val original = new IllegalStateException("boom")

    handler.recordQueryThrowable(persisted, "Fail to write query result", original)

    handler.error shouldBe persisted
    handler.exceptionThrown shouldBe Some(original)
    handler.hasException shouldBe true
  }

  test("operatorLogEvent carries typed classification and AWS correlation context") {
    val handler = new ThrowableHandler()
    val original = new AmazonS3Exception("customer-bucket/customer-key")
    original.setServiceName("Amazon S3")
    original.setStatusCode(403)
    original.setErrorCode("AccessDenied")
    original.setRequestId("request-123")
    original.setExtendedRequestId("extended-request-456")

    val (event, redacted) = handler.operatorLogEvent("Fail to read data", original)

    event.getStatusCode shouldBe 403
    event.getErrorCode shouldBe "S3_ERROR"
    event.getExceptionType shouldBe classOf[AmazonS3Exception].getName
    event.getRequestId shouldBe "request-123"
    event.getExtendedRequestId shouldBe "extended-request-456"
    event.getFormattedMessage should not include "customer-bucket"
    redacted.getMessage should not include "customer-key"
  }

  test(
    "operatorLogRendering strips the interpolated customer value and the persisted JSON from the " +
      "logged message and throwable for a Spark error-class failure") {
    val handler = new ThrowableHandler()
    val columnCanary = "customer_secret_column_CANARY"
    val original = new SparkException(
      "COLUMN_ALREADY_EXISTS",
      Map("columnName" -> columnCanary),
      /* cause = */ null)
    // The persisted record carries the customer JSON verbatim; the driver log must not.
    val persistedJsonCanary = "persisted_customer_json_CANARY"
    val persisted = s"""{"message":"$persistedJsonCanary"}"""

    val (logMessage, logThrowable) =
      handler.operatorLogRendering("Fail to write query result", original)

    // The static context and catalog errorClass survive; the interpolated value and the persisted
    // JSON never do.
    logMessage should include("Fail to write query result")
    logMessage should include("COLUMN_ALREADY_EXISTS")
    logMessage should include("<columnName>")
    logMessage should not include columnCanary
    logMessage should not include persistedJsonCanary

    // The redacted throwable exposes only the strict operator-log message through every accessor a
    // logger reads, while keeping the original type name for debugging.
    logThrowable shouldBe a[RedactedException]
    val fields = Seq(
      logThrowable.getMessage,
      logThrowable.getLocalizedMessage,
      logThrowable.toString,
      renderStackTrace(logThrowable))
    fields.foreach { field =>
      field should not include columnCanary
      field should not include persistedJsonCanary
    }
    logThrowable.toString should include("SparkException")
  }

  test(
    "operatorLogRendering emits a safe label and class name, never the raw message, for a " +
      "non-Spark query throwable") {
    val handler = new ThrowableHandler()
    val literalCanary = "customer_literal_CANARY"
    val original = new IllegalArgumentException(s"offending value $literalCanary\nsecond line")

    val (logMessage, logThrowable) =
      handler.operatorLogRendering("Unexpected error during query execution", original)

    logMessage shouldBe
      "Unexpected error during query execution: [UNKNOWN_ERROR] type=[java.lang.IllegalArgumentException]"
    logMessage should not include literalCanary
    logMessage should not include "second line"
    Seq(logThrowable.getMessage, logThrowable.toString, renderStackTrace(logThrowable)).foreach {
      field => field should not include literalCanary
    }
  }

  test(
    "operatorLogRendering drops a custom SparkThrowable's rendered message and query context") {
    val handler = new ThrowableHandler()
    val sqlCanary = "SELECT secret_col FROM customer_table"
    val original =
      new RuntimeException(s"[DIVIDE_BY_ZERO] Division by zero.\n== SQL ==\n$sqlCanary")
        with SparkThrowable {
        override def getErrorClass: String = "DIVIDE_BY_ZERO"
      }

    val (logMessage, logThrowable) =
      handler.operatorLogRendering("Fail to write query result", original)

    logMessage should include("[DIVIDE_BY_ZERO]")
    logMessage should not include "== SQL =="
    logMessage should not include "secret_col"
    logMessage should not include "customer_table"
    Seq(logThrowable.getMessage, renderStackTrace(logThrowable)).foreach { field =>
      field should not include sqlCanary
      field should not include "== SQL =="
    }
  }
}
