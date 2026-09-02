/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql

import com.amazonaws.services.glue.model.AWSGlueException
import com.amazonaws.services.s3.model.AmazonS3Exception
import org.opensearch.{OpenSearchException, OpenSearchSecurityException}
import org.opensearch.action.DocWriteRequest.OpType
import org.opensearch.action.bulk.{BulkItemResponse, BulkResponse}
import org.opensearch.action.bulk.BulkItemResponse.Failure
import org.opensearch.flint.core.storage.OpenSearchBulkWriteException
import org.opensearch.rest.RestStatus
import org.scalatest.matchers.should.Matchers

import org.apache.spark.{SparkException, SparkFunSuite, SparkThrowable}
import org.apache.spark.sql.ErrorSanitizer.ErrorCode
import org.apache.spark.sql.catalyst.ExtendedAnalysisException
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.catalyst.trees.Origin
import org.apache.spark.sql.types.StringType

/**
 * Covers the two halves of the error contract separately: the message (what is read) and the
 * classification (what a consumer branches on). The message itself has two audiences with two
 * policies -- [[ErrorSanitizer.customerMessage]] keeps the actionable diagnostic for the
 * persisted / forwarded record, while [[ErrorSanitizer.operatorLogMessage]] keeps only
 * classification-stable, provably non-customer-derived detail for the driver logs (the catalog
 * errorClass, its static message template with `<param>` placeholders left un-interpolated, and a
 * safe cause class name) -- and the classification must not depend on the wording of either.
 */
class ErrorSanitizerTest extends SparkFunSuite with Matchers {

  private def bulkWriteException(
      status: RestStatus,
      cause: Exception = new OpenSearchSecurityException("no permissions"))
      : OpenSearchBulkWriteException = {
    val item =
      new BulkItemResponse(0, OpType.INDEX, new Failure("myindex", "doc-1", cause, status))
    OpenSearchBulkWriteException.from("myindex", new BulkResponse(Array(item), 100L), _ => true)
  }

  // ---- classification ----

  test("classify maps a forbidden bulk write to access-denied with its status code") {
    val classification = ErrorSanitizer.classify(bulkWriteException(RestStatus.FORBIDDEN))

    classification.errorCode shouldBe ErrorCode.OpenSearchWriteAccessDenied
    classification.statusCode shouldBe Some(403)
  }

  test("classify keeps a non-security 403 as a general OpenSearch write error") {
    // OpenSearch 2.6 reconstructs server errors as generic OpenSearchException instances and keeps
    // the original server type in this structured prefix. A readonly index produces this 403 shape;
    // status alone must not turn it into a customer authorization failure.
    val clusterBlock = new OpenSearchException(
      "OpenSearch exception [type=cluster_block_exception, reason=index read-only]")
    val e = bulkWriteException(RestStatus.FORBIDDEN, clusterBlock)

    val classification = ErrorSanitizer.classify(e)
    val message = ErrorSanitizer.customerMessage(e)

    classification.errorCode shouldBe ErrorCode.OpenSearchWriteError
    classification.statusCode shouldBe Some(403)
    message should include("type=cluster_block_exception")
    message should not include "type=authorization_exception"
    message should not include "reason=OpenSearch exception"
  }

  test("classify distinguishes a non-authorization bulk write failure from access denied") {
    val classification =
      ErrorSanitizer.classify(bulkWriteException(RestStatus.TOO_MANY_REQUESTS))

    classification.errorCode shouldBe ErrorCode.OpenSearchWriteError
    classification.statusCode shouldBe Some(429)
  }

  test("classify finds a typed failure wrapped inside a SparkException") {
    // Real shape: a write failure surfaces to the driver wrapped in a task-failure SparkException.
    val wrapped =
      new SparkException(
        "Job aborted due to stage failure",
        bulkWriteException(RestStatus.FORBIDDEN))

    val classification = ErrorSanitizer.classify(wrapped)

    classification.errorCode shouldBe ErrorCode.OpenSearchWriteAccessDenied
    classification.statusCode shouldBe Some(403)
  }

  test("classify reads the status from a typed AWS exception rather than its message") {
    val s3 = new AmazonS3Exception("Access Denied")
    s3.setStatusCode(403)
    s3.setServiceName("Amazon S3")

    val classification = ErrorSanitizer.classify(s3)

    classification.errorCode shouldBe ErrorCode.S3Error
    classification.statusCode shouldBe Some(403)
  }

  test(
    "classify maps a Glue AccessDeniedException to access-denied even when the SDK status is 400") {
    // Glue reports authorization failures with the concrete AccessDeniedException type but an HTTP
    // status of 400, so the status alone would misclassify it as a generic Glue error.
    val glue = new com.amazonaws.services.glue.model.AccessDeniedException("denied")
    glue.setStatusCode(400)
    glue.setServiceName("AWSGlue")

    val classification = ErrorSanitizer.classify(glue)

    classification.errorCode shouldBe ErrorCode.GlueAccessDenied
    classification.statusCode shouldBe Some(400)
  }

  test("operator logging keeps typed Glue service context and drops the raw message") {
    val glue = new AWSGlueException("customer_table_CANARY")
    glue.setServiceName("AWSGlue")
    glue.setStatusCode(500)
    glue.setErrorCode("InternalServiceException")

    val message = ErrorSanitizer.operatorLogMessage(glue)

    message shouldBe
      "serviceName=[AWSGlue], statusCode=[500], errorCode=[InternalServiceException]"
    message should not include "customer_table_CANARY"
  }

  test("operatorLogContext returns typed AWS request identifiers") {
    val s3 = new AmazonS3Exception("customer-bucket_CANARY")
    s3.setRequestId("request-123")
    s3.setExtendedRequestId("extended-request-456")

    val context = ErrorSanitizer.operatorLogContext(new SparkException("wrapper", s3))

    context.requestId shouldBe Some("request-123")
    context.extendedRequestId shouldBe Some("extended-request-456")
  }

  test("classify maps a SparkThrowable that is not a query exception to a spark query error") {
    val e = new RuntimeException("boom") with SparkThrowable {
      override def getErrorClass: String = "SOME_SPARK_ERROR"
    }

    ErrorSanitizer.classify(e).errorCode shouldBe ErrorCode.SparkQueryError
  }

  test("classify treats an authorization failure in a mixed bulk write as access denied") {
    val forbidden = new BulkItemResponse(
      0,
      OpType.INDEX,
      new Failure(
        "myindex",
        "doc-1",
        new OpenSearchSecurityException("denied"),
        RestStatus.FORBIDDEN))
    val throttled = new BulkItemResponse(
      1,
      OpType.INDEX,
      new Failure("myindex", "doc-2", new RuntimeException("busy"), RestStatus.TOO_MANY_REQUESTS))
    // The throttled item is listed first to prove 403 wins irrespective of item order.
    val e = OpenSearchBulkWriteException.from(
      "myindex",
      new BulkResponse(Array(throttled, forbidden), 100L),
      _ => true)

    val classification = ErrorSanitizer.classify(e)
    classification.errorCode shouldBe ErrorCode.OpenSearchWriteAccessDenied
    classification.statusCode shouldBe Some(403)
  }

  test("classify separates a syntax error from a general analysis error") {
    ErrorSanitizer
      .classify(
        new ExtendedAnalysisException(
          message = "[UNRESOLVED_COLUMN] cannot resolve column",
          line = Some(1),
          startPosition = Some(1),
          plan = Some(LocalRelation(AttributeReference("c", StringType)()))))
      .errorCode shouldBe ErrorCode.QueryAnalysisError
  }

  test("classify falls back to unknown without a status for an unrecognized throwable") {
    val classification = ErrorSanitizer.classify(new IllegalStateException("something broke"))

    classification.errorCode shouldBe ErrorCode.Unknown
    classification.statusCode shouldBe None
  }

  test("classify tolerates a self-referencing cause chain") {
    // A throwable whose cause is itself would loop a naive walker.
    val selfReferencing = new RuntimeException("loop") {
      override def getCause: Throwable = this
    }

    ErrorSanitizer.classify(selfReferencing).errorCode shouldBe ErrorCode.Unknown
  }

  // ---- customer message: keeps the actionable diagnostic, drops the query itself ----

  test("customerMessage keeps the bulk write classification but drops per-item failure text") {
    val documentValue = "customer-secret-field-value"
    val e = bulkWriteException(
      RestStatus.FORBIDDEN,
      new OpenSearchSecurityException(s"no permissions for $documentValue"))

    val message = ErrorSanitizer.customerMessage(e)

    message should include("statusCode=[403]")
    message should include(
      "type=security_exception, reason=OpenSearch exception [type=authorization_exception]")
    message should not include documentValue
    message should not include "myindex"
  }

  test("customerMessage returns structured fields for an AWS exception, not its raw message") {
    val s3 = new AmazonS3Exception("Access Denied for s3://customer-bucket/secret-prefix/file")
    s3.setStatusCode(403)
    s3.setServiceName("Amazon S3")
    s3.setErrorCode("AccessDenied")

    val message = ErrorSanitizer.customerMessage(s3)

    message should include("statusCode=[403]")
    message should include("errorCode=[AccessDenied]")
    // The raw message can embed customer bucket and key names, so it must not be carried over.
    message should not include "customer-bucket"
    message should not include "secret-prefix"
  }

  test(
    "customerMessage keeps a non-S3 AWS message so a deliberately curated one is not discarded") {
    // processQueryException replaces a Glue access-denied message with an actionable explanation.
    // Reducing every AWS exception to an error code would throw that away, so other AWS families
    // keep their first line.
    val glue =
      new AWSGlueException("Access denied in AWS Glue service. Please check permissions.")
    glue.setStatusCode(400)
    glue.setServiceName("AWSGlue")

    ErrorSanitizer.customerMessage(glue) should include(
      "Access denied in AWS Glue service. Please check permissions.")
  }

  test(
    "customerMessage keeps the analysis diagnostic (identifier and suggestion) but drops the plan " +
      "and appends no sqlState") {
    val planMarker = "recipientAccountId_CANARY_ACCOUNT_A"
    val plan = LocalRelation(AttributeReference(planMarker, StringType)())
    val e = new ExtendedAnalysisException(
      message = "[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column or function parameter with name " +
        "`alias_1`.`col` cannot be resolved. Did you mean one of the following? " +
        "[`alias_0`.`col`].",
      line = Some(1),
      startPosition = Some(295),
      plan = Some(plan))

    // Precondition: getMessage really does append the plan tree that carries customer content.
    e.getMessage should include(planMarker)

    val customer = ErrorSanitizer.customerMessage(e)

    // The useful diagnostic a customer needs is retained ...
    customer should include("UNRESOLVED_COLUMN.WITH_SUGGESTION")
    customer should include("Did you mean one of the following?")
    customer should include("`alias_0`.`col`")
    // ... while the logical plan and its query-derived markers are gone ...
    customer should not include planMarker
    customer should not include "LocalRelation"
    // ... and no SQL state token is appended to customer-visible text.
    customer should not include "sqlState=["
  }

  test("customerMessage keeps the parser diagnostic but drops the raw == SQL == query block") {
    val customerSql = "SELECT secret_col FROM t WHERE x LIKE '%example-secret%' BADTOKEN"
    val origin = Origin(line = Some(1), startPosition = Some(40))
    val e = new ParseException(
      command = Some(customerSql),
      message = "Syntax error at or near 'BADTOKEN'",
      start = origin,
      stop = origin)

    // Precondition: getMessage embeds the verbatim SQL in a == SQL == block.
    e.getMessage should include("== SQL ==")
    e.getMessage should include("secret_col")

    val customer = ErrorSanitizer.customerMessage(e)

    // The parser detail is retained ...
    customer should include("Syntax error at or near")
    customer should include("BADTOKEN")
    // ... but the verbatim query and its == SQL == block are gone, and no sqlState is appended.
    customer should not include "== SQL =="
    customer should not include "secret_col"
    customer should not include "example-secret"
    customer should not include "sqlState=["
  }

  test(
    "customerMessage preserves a generic SparkException first-line diagnostic (replaceable " +
      "expression) so downstream message rules keep matching, without appending sqlState") {
    // Source-faithful reproduction: CheckAnalysis raises this exact failure via
    // SparkException.internalError(...) for an unresolved RuntimeReplaceable. The construction
    // matches Spark 3.5.1 CheckAnalysis.scala; only the operand SQL text is a synthetic canary.
    val e = SparkException.internalError(
      "Cannot resolve the runtime replaceable expression \"synthetic_fn(canary_operand)\". " +
        "The replacement is unresolved: \"canary_operand\".")

    val message = ErrorSanitizer.customerMessage(e)

    // The legacy phrase downstream translations key on survives verbatim ...
    message should include("Cannot resolve the runtime replaceable expression")
    // ... under the stable INTERNAL_ERROR class, and no sqlState token is appended.
    message should include("[INTERNAL_ERROR]")
    message should not include "sqlState=["
  }

  test(
    "customerMessage drops multi-line query context below the first line of a SparkThrowable") {
    // Even when a Spark-native message appends a query fragment on later lines, only the first-line
    // diagnostic is kept.
    val e = new RuntimeException(
      "[DIVIDE_BY_ZERO] Division by zero.\n== SQL ==\nSELECT secret_value / 0 FROM customer_table")
      with SparkThrowable {
      override def getErrorClass: String = "DIVIDE_BY_ZERO"
    }

    val message = ErrorSanitizer.customerMessage(e)

    message should include("[DIVIDE_BY_ZERO] Division by zero.")
    message should not include "== SQL =="
    message should not include "secret_value"
    message should not include "customer_table"
  }

  test("customerMessage fails closed when policy evaluation itself throws") {
    val secret = "customer-secret-literal"
    val hostile = new RuntimeException(secret) {
      override def getMessage: String = throw new IllegalStateException("cannot render")
    }

    val message = ErrorSanitizer.customerMessage(hostile)

    message shouldBe ErrorSanitizer.RedactedFallbackMessage
    message should not include secret
  }

  test("customerMessage returns an empty string for a null message without throwing") {
    ErrorSanitizer.customerMessage(new RuntimeException()) shouldBe ""
    ErrorSanitizer.customerMessage(new NullPointerException()) shouldBe ""
  }

  test("customerMessage leaves a single-line unrecognized message untouched") {
    ErrorSanitizer.customerMessage(
      new IllegalArgumentException("bad arg value 42")) shouldBe "bad arg value 42"
  }

  test("customerMessage keeps only the first line of an unrecognized multiline message") {
    ErrorSanitizer.customerMessage(
      new RuntimeException("summary line\ndetail-line\nmore-detail")) shouldBe "summary line"
  }

  // ---- operator log message: strict, classification-only ----

  test(
    "operatorLogMessage keeps the static catalog template but never the interpolated parameter " +
      "value for a Spark error-class exception") {
    // A real Spark 3.5.1 SparkThrowable built through the error-class constructor, so getMessage is
    // rendered from the catalog with the parameter interpolated -- the path that would leak the
    // identifier. The log message keeps the static template (which names `<columnName>` as a
    // placeholder) but never the interpolated value.
    val secret = "customer_secret_column_CANARY_ACCOUNT_A"
    val e = new SparkException(
      "COLUMN_ALREADY_EXISTS",
      Map("columnName" -> secret),
      /* cause = */ null)

    // Precondition: the rendered message really does interpolate the secret parameter value.
    e.getMessage should include(secret)

    val logMessage = ErrorSanitizer.operatorLogMessage(e)

    logMessage shouldBe
      "[COLUMN_ALREADY_EXISTS] The column <columnName> already exists. " +
      "Consider to choose another name or rename the existing column."
    // The static placeholder is kept as useful, non-customer diagnostic ...
    logMessage should include("<columnName>")
    // ... while the interpolated parameter value never survives.
    logMessage should not include secret
  }

  test(
    "operatorLogMessage keeps the unresolved-column template and the static suggestion phrase but " +
      "never the offending identifier or the suggested value") {
    // The suggestion phrase ("Did you mean one of the following?") is authored catalog text, so it
    // is a safe static diagnostic; only the interpolated `<objectName>` and `<proposal>` values are
    // customer-derived and must not survive.
    val offending = "customer_col_CANARY_A"
    val suggestion = "customer_col_CANARY_B"
    val e = new RuntimeException(
      s"[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column or function parameter with name `$offending` " +
        s"cannot be resolved. Did you mean one of the following? [`$suggestion`].")
      with SparkThrowable {
      override def getErrorClass: String = "UNRESOLVED_COLUMN.WITH_SUGGESTION"
    }

    val logMessage = ErrorSanitizer.operatorLogMessage(e)

    logMessage shouldBe
      "[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column or function parameter with name <objectName> " +
      "cannot be resolved. Did you mean one of the following? [<proposal>]."
    logMessage should include("Did you mean one of the following?")
    logMessage should include("<objectName>")
    logMessage should include("<proposal>")
    logMessage should not include offending
    logMessage should not include suggestion
  }

  test(
    "operatorLogMessage keeps the INTERNAL_ERROR template placeholder and a safe cause class but " +
      "never the wrapped message value") {
    // INTERNAL_ERROR's template is only the `<message>` placeholder, so the genuinely useful,
    // provably-safe diagnostic for it is the structured cause class name -- a code identifier, not
    // a customer value. The interpolated message parameter (which carries the wrapped detail) must
    // not survive.
    val secret = "internal_detail_CANARY"
    val cause = new IllegalStateException("hostile cause detail canary")
    val e = new SparkException("INTERNAL_ERROR", Map("message" -> secret), cause)

    // Precondition: the rendered message really does interpolate the secret parameter value.
    e.getMessage should include(secret)

    val logMessage = ErrorSanitizer.operatorLogMessage(e)

    logMessage shouldBe "[INTERNAL_ERROR] <message> cause=[java.lang.IllegalStateException]"
    logMessage should not include secret
    logMessage should not include "hostile cause detail"
  }

  test(
    "operatorLogMessage emits only the bracketed class for a custom errorClass with no catalog " +
      "template") {
    // A custom SparkThrowable can return an arbitrary errorClass that is not in the catalog;
    // getMessageTemplate throws for it, so the label falls back to the bare bracketed class rather
    // than emitting anything unvalidated. This matches the previously shipped behavior.
    val e = new RuntimeException("boom") with SparkThrowable {
      override def getErrorClass: String = "SOME_CUSTOM_CLASS_NOT_IN_CATALOG"
    }

    ErrorSanitizer.operatorLogMessage(e) shouldBe "[SOME_CUSTOM_CLASS_NOT_IN_CATALOG]"
  }

  test(
    "operatorLogMessage keeps the static template and drops a SparkThrowable's rendered message " +
      "and query context entirely") {
    val e = new RuntimeException(
      "[DIVIDE_BY_ZERO] Division by zero.\n== SQL ==\nSELECT secret_value / 0 FROM customer_table")
      with SparkThrowable {
      override def getErrorClass: String = "DIVIDE_BY_ZERO"
    }

    val logMessage = ErrorSanitizer.operatorLogMessage(e)

    logMessage shouldBe
      "[DIVIDE_BY_ZERO] Division by zero. Use `try_divide` to tolerate divisor being 0 and " +
      "return NULL instead. If necessary set <config> to \"false\" to bypass this error."
    // The static config placeholder is kept, never an interpolated config value ...
    logMessage should include("<config>")
    // ... and none of the throwable's own rendered message or query context leaks.
    logMessage should not include "== SQL =="
    logMessage should not include "secret_value"
    logMessage should not include "customer_table"
  }

  test(
    "operatorLogMessage reduces a hand-built analysis exception to a bare label, recovering no " +
      "token from its free-text message") {
    val planMarker = "CANARY_ACCOUNT_A"
    val plan = LocalRelation(AttributeReference(s"recipientAccountId_$planMarker", StringType)())
    val e = new ExtendedAnalysisException(
      message = "[UNRESOLVED_COLUMN] cannot resolve `arn`",
      line = Some(1),
      startPosition = Some(295),
      plan = Some(plan))

    val logMessage = ErrorSanitizer.operatorLogMessage(e)

    logMessage shouldBe "[SPARK_ERROR]"
    logMessage should not include "UNRESOLVED_COLUMN"
    logMessage should not include "arn"
    logMessage should not include planMarker
    logMessage should not include "LocalRelation"
  }

  test(
    "operatorLogMessage keeps a bounded, safe cause class for a generic SparkException with no " +
      "errorClass, without any cause message") {
    val cause = new java.util.regex.PatternSyntaxException("hostile detail", "[", 0)
    val e = new SparkException("wrapper message with secret detail", cause)

    val logMessage = ErrorSanitizer.operatorLogMessage(e)

    logMessage shouldBe "[SPARK_ERROR] cause=[java.util.regex.PatternSyntaxException]"
    // No message content from either the wrapper or the cause survives.
    logMessage should not include "wrapper message"
    logMessage should not include "hostile detail"
  }

  test("operatorLogMessage is bounded against a cause whose getCause throws") {
    val e = new SparkException("boom") {
      override def getCause: Throwable =
        throw new RuntimeException("secondary failure canary")
    }

    val logMessage = ErrorSanitizer.operatorLogMessage(e)

    // No cause token (the throwing accessor is treated as "no cause"), and no secondary canary.
    logMessage shouldBe "[SPARK_ERROR]"
    logMessage should not include "secondary failure canary"
  }

  test("operatorLogMessage is bounded against a self-referencing cause") {
    val e = new SparkException("boom") {
      override def getCause: Throwable = this
    }

    ErrorSanitizer.operatorLogMessage(e) shouldBe "[SPARK_ERROR]"
  }

  test("operatorLogMessage keeps the structured, safe fields for an OpenSearch bulk write") {
    val documentValue = "customer-secret-field-value"
    val e = bulkWriteException(
      RestStatus.FORBIDDEN,
      new OpenSearchSecurityException(s"no permissions for $documentValue"))

    val logMessage = ErrorSanitizer.operatorLogMessage(e)

    logMessage should include("statusCode=[403]")
    logMessage should not include documentValue
    logMessage should not include "myindex"
  }

  test(
    "operatorLogMessage emits a safe label and class name, never the message, for a chain with " +
      "no recognized type") {
    val secret = "customer_literal_CANARY"
    val e = new IllegalStateException(s"offending value $secret\nplan line")

    val logMessage = ErrorSanitizer.operatorLogMessage(e)

    logMessage shouldBe "[UNKNOWN_ERROR] type=[java.lang.IllegalStateException]"
    logMessage should not include secret
    logMessage should not include "plan line"
  }

  test(
    "operatorLogMessage does not read getMessage on the unrecognized floor, so a hostile " +
      "getMessage cannot force a secondary failure") {
    val secret = "customer-secret-literal"
    // A non-Spark hostile throwable whose getMessage throws. The unrecognized floor reads only the
    // class name, so it returns the safe label rather than invoking getMessage and failing closed.
    val hostile = new RuntimeException(secret) {
      override def getMessage: String = throw new IllegalStateException("cannot render")
    }

    val logMessage = ErrorSanitizer.operatorLogMessage(hostile)

    logMessage should startWith("[UNKNOWN_ERROR] type=[")
    logMessage should not include secret
    logMessage should not include "cannot render"
  }
}
