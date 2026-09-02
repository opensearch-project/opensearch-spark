/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql

import scala.collection.JavaConverters._

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.opensearch.flint.common.model.FlintStatement
import org.opensearch.flint.core.logging.{CustomLogging, ExceptionMessages, OperationMessage}
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.SparkException
import org.apache.spark.sql.ErrorSanitizer.ErrorCode
import org.apache.spark.sql.test.SharedSparkSessionBase

/**
 * Companion to [[ErrorSanitizerTest]] and the redaction tests in [[FlintREPLTest]], which build
 * the analysis/parse/runtime exceptions by hand. This suite instead provokes the real Spark 3.5.1
 * exceptions from a live [[SparkSession]] so the sanitizer is exercised against the exact message
 * shapes Spark actually emits (including the appended logical plan and the {@code == SQL ==}
 * fragment) rather than a reconstruction of them.
 *
 * The two message audiences are asserted separately. The customer / persisted message
 * ([[ErrorSanitizer.customerMessage]], which backs the persisted error JSON) keeps the actionable
 * diagnostic but drops the query itself (the logical plan and the {@code == SQL ==} block) and
 * never appends a SQL state. The operator log message ([[ErrorSanitizer.operatorLogMessage]], the
 * one [[FlintREPL.redactThrowable]] wraps for the driver logs) keeps only provably
 * non-customer-derived detail: the catalog errorClass and its static message template, whose
 * `<param>` placeholders are left un-interpolated so no offending identifier, suggestion value,
 * literal, or query text can appear.
 */
class ErrorSanitizerRealSparkTest
    extends QueryTest
    with SharedSparkSessionBase
    with Matchers
    with MockitoSugar {

  /** Runs a statement expected to fail and returns the root cause, mirroring the driver path. */
  private def failWith(sql: String): Throwable = {
    val raised = intercept[Throwable](spark.sql(sql).collect())
    FlintREPL.getRootCause(raised)
  }

  private def persistedJson(t: Throwable): String =
    FlintREPL.processQueryException(t, mock[FlintStatement])

  private def operationMessage(message: String, t: Throwable): OperationMessage = {
    val classification = ErrorSanitizer.classify(t)
    val context = ErrorSanitizer.operatorLogContext(t)
    new OperationMessage(
      message,
      classification.statusCode.map(Int.box).orNull,
      classification.errorCode,
      t.getClass.getName,
      context.requestId.orNull,
      context.extendedRequestId.orNull)
  }

  /**
   * Invokes the real production [[CustomLogging.constructLogEventMap]] (the deterministic method
   * that assembles every field a log event carries, including `body.message`, `exception.type`,
   * and `exception.message`). It is `protected` and lives in another package, so it is reached by
   * reflection rather than intercepting a live Log4j appender; this exercises the exact
   * production code path without the brittleness of appender capture.
   */
  private def constructLogEventMap(
      content: Object,
      throwable: Throwable): java.util.Map[String, Object] = {
    val method = classOf[CustomLogging].getDeclaredMethod(
      "constructLogEventMap",
      classOf[String],
      classOf[Object],
      classOf[Throwable])
    method.setAccessible(true)
    method
      .invoke(null, "ERROR", content, throwable)
      .asInstanceOf[java.util.Map[String, Object]]
  }

  test(
    "real unresolved-column AnalysisException: customer keeps the diagnostic, drops the plan") {
    spark.sql("SELECT 1 AS id, 'x' AS name").createOrReplaceTempView("real_probe_vals")
    val column = "nonexistent_col_realspark_5551"
    val analysis = failWith(s"SELECT $column FROM real_probe_vals")

    // Precondition: the real message appends the resolved logical plan tree and names the column.
    analysis.getMessage should include("Project")
    analysis.getMessage should include(column)
    analysis.isInstanceOf[AnalysisException] shouldBe true

    // Customer / persisted message: keeps the actionable diagnostic (errorClass, the offending
    // column, and the "Did you mean" suggestion) ...
    val customer = ErrorSanitizer.customerMessage(analysis)
    customer should include("UNRESOLVED_COLUMN")
    customer should include(column)
    customer should include("Did you mean")
    // ... but the appended logical plan tree and its node names are gone, and no sqlState is added.
    customer should not include "Project"
    customer should not include "SubqueryAlias"
    customer should not include "LocalRelation"
    customer should not include "sqlState=["

    // Operator log message: the catalog errorClass plus its static template. The static suggestion
    // phrase is authored catalog text and is kept, but the offending column, the interpolated
    // `<objectName>`/`<proposal>` values, and the plan are gone.
    val log = ErrorSanitizer.operatorLogMessage(analysis)
    log should include("UNRESOLVED_COLUMN")
    log should include("Did you mean one of the following?")
    log should include("<objectName>")
    log should not include column
    log should not include "Project"

    ErrorSanitizer.classify(analysis).errorCode shouldBe ErrorCode.QueryAnalysisError

    // The persisted JSON carries the customer diagnostic (column + suggestion) but never the plan.
    val json = persistedJson(analysis)
    json should not include "Project"
    json should not include "LocalRelation"
    json should include(column)
    json should include(""""errorCode":"QUERY_ANALYSIS_ERROR"""")
    json should include(
      """"exception.type":"org.apache.spark.sql.catalyst.ExtendedAnalysisException"""")
  }

  test(
    "real ParseException: customer keeps the parser detail but drops the == SQL == query block") {
    spark.sql("SELECT 1 AS id, 'x' AS name").createOrReplaceTempView("real_probe_vals")
    // The canary lives in a string literal, so it appears only in the verbatim == SQL == echo, not
    // in the syntax diagnostic. The trailing token is what the parser actually reports as offending.
    val rawSqlCanary = "raw_sql_canary_6662"
    val offendingToken = "BOGUSTOKEN_6662"
    val parse =
      failWith(s"SELECT id FROM real_probe_vals WHERE name = '$rawSqlCanary' $offendingToken")

    parse.getMessage should include("== SQL ==")
    parse.getMessage should include(rawSqlCanary)

    // Customer message: the parser diagnostic survives; the verbatim query does not, and no sqlState.
    val customer = ErrorSanitizer.customerMessage(parse)
    customer should include("PARSE_SYNTAX_ERROR")
    customer should not include "== SQL =="
    customer should not include rawSqlCanary
    customer should not include "sqlState=["

    // Operator log message: the catalog errorClass plus its static template ("Syntax error at or
    // near <error><hint>."). The verbatim query and offending token value never appear.
    val log = ErrorSanitizer.operatorLogMessage(parse)
    log should include("PARSE_SYNTAX_ERROR")
    log should include("Syntax error at or near")
    log should include("<error>")
    log should not include rawSqlCanary
    log should not include offendingToken

    ErrorSanitizer.classify(parse).errorCode shouldBe ErrorCode.QuerySyntaxError

    val json = persistedJson(parse)
    json should not include rawSqlCanary
    json should not include "== SQL =="
    json should include(""""errorCode":"QUERY_SYNTAX_ERROR"""")
  }

  test(
    "real runtime SparkThrowable: customer keeps the first-line diagnostic, drops the SQL frag") {
    spark.conf.set("spark.sql.ansi.enabled", "true")
    val runtime =
      try failWith("SELECT 1 / 0 AS x")
      finally spark.conf.unset("spark.sql.ansi.enabled")

    // Precondition: the real ANSI arithmetic failure appends the offending SQL after a newline.
    runtime.getMessage should include("== SQL")
    runtime.isInstanceOf[org.apache.spark.SparkThrowable] shouldBe true

    // Customer message: the first-line diagnostic survives; the appended query fragment does not,
    // and no sqlState token is added.
    val customer = ErrorSanitizer.customerMessage(runtime)
    customer should include("[DIVIDE_BY_ZERO]")
    customer should include("Division by zero")
    customer should not include "== SQL"
    customer should not include "SELECT"
    customer should not include "sqlState=["

    // Operator log message: the errorClass plus its static template, keeping the `<config>`
    // placeholder but no interpolated value and none of the appended query fragment.
    val log = ErrorSanitizer.operatorLogMessage(runtime)
    log should startWith("[DIVIDE_BY_ZERO] Division by zero.")
    log should include("<config>")
    log should not include "== SQL"
    log should not include "SELECT"

    ErrorSanitizer.classify(runtime).errorCode shouldBe ErrorCode.SparkQueryError

    val json = persistedJson(runtime)
    json should not include "SELECT 1 / 0"
    json should include(""""errorCode":"SPARK_QUERY_ERROR"""")
  }

  test(
    "source-faithful runtime replaceable expression: the legacy phrase survives in the persisted " +
      "record for downstream matching, but not in the operator log") {
    // CheckAnalysis raises this exact failure for an unresolved RuntimeReplaceable via
    // SparkException.internalError(...). It is a "should not happen" analyzer safety net that a live
    // query cannot deterministically trigger, so this reproduces the construction faithfully (same
    // type org.apache.spark.SparkException, same INTERNAL_ERROR class, same leading phrase); only
    // the operand SQL text is a synthetic canary. Remaining gap: not provoked end to end by a query.
    val operandCanary = "synthetic_fn(canary_operand_replaceable)"
    val replaceable = SparkException.internalError(
      "Cannot resolve the runtime replaceable expression '" + operandCanary + "'. " +
        "The replacement is unresolved: 'canary_operand_replaceable'.")

    // Customer / persisted message keeps the leading phrase downstream rules key on.
    val customer = ErrorSanitizer.customerMessage(replaceable)
    customer should include("Cannot resolve the runtime replaceable expression")
    customer should not include "sqlState=["

    // Operator log keeps the INTERNAL_ERROR class and its template, which is only the `<message>`
    // placeholder -- so the operand canary and the interpolated message text never appear.
    val log = ErrorSanitizer.operatorLogMessage(replaceable)
    log shouldBe "[INTERNAL_ERROR] <message>"
    log should not include "canary_operand_replaceable"
    log should not include "Cannot resolve the runtime replaceable expression"

    val json = persistedJson(replaceable)
    // The persisted record carries the phrase under the real SparkException type, so the supplied
    // downstream translation rule keeps matching.
    val downstreamRule =
      """.*Cannot resolve the runtime replaceable expression.*""".r
    downstreamRule.findFirstIn(json) shouldBe defined
    json should include(""""exception.type":"org.apache.spark.SparkException"""")
    json should include(""""errorCode":"SPARK_QUERY_ERROR"""")
  }

  test("getRootCause and processQueryException are cycle-safe on a self-referencing cause") {
    // A throwable whose cause is itself. Before the cycle guard, getRootCause recursed until it
    // threw StackOverflowError on the error-handling path, masking the original failure.
    val selfReferencing = new RuntimeException("cycle_realspark_7773") {
      override def getCause: Throwable = this
    }

    noException should be thrownBy FlintREPL.getRootCause(selfReferencing)
    FlintREPL.getRootCause(selfReferencing) should be theSameInstanceAs selfReferencing

    val json = persistedJson(selfReferencing)
    json should include(""""errorCode":"UNKNOWN_ERROR"""")
  }

  test(
    "CustomLogging event map for a redacted real analysis exception carries no query canaries") {
    // End-to-end proof over the actual logging construction path: a real analysis failure is
    // redacted exactly as the production catch site does (FlintREPL.redactThrowable -> the strict
    // operatorLogMessage), then handed to the real CustomLogging.constructLogEventMap. Every field
    // the log event exposes -- body.message, exception.message, exception.type, and the rendered
    // throwable (toString header + stack trace) -- is asserted free of any query-derived content.
    spark.sql("SELECT 1 AS id, 'x' AS name").createOrReplaceTempView("real_probe_log")
    val column = "nonexistent_col_customlogging_8884"
    val analysis = failWith(s"SELECT $column FROM real_probe_log")

    // Reproduce the production log catch-site redaction exactly, including the "prefix: <log>"
    // message shape passed to CustomLogging.logError.
    val redacted = FlintREPL.redactThrowable(analysis)
    val logMessage =
      s"${ExceptionMessages.QueryAnalysisErrorPrefix}: ${ErrorSanitizer.operatorLogMessage(analysis)}"
    val logEventMap = constructLogEventMap(operationMessage(logMessage, analysis), redacted)

    val body = logEventMap.get("body").asInstanceOf[java.util.Map[String, Object]]
    val attributes = logEventMap.get("attributes").asInstanceOf[java.util.Map[String, Object]]
    val bodyMessage = body.get("message").asInstanceOf[String]
    val exceptionMessage = attributes.get("exception.message").asInstanceOf[String]
    val exceptionType = attributes.get("exception.type").asInstanceOf[String]

    // Render the throwable exactly as log4j would: the toString() header plus the stack-trace body.
    val sw = new java.io.StringWriter()
    redacted.printStackTrace(new java.io.PrintWriter(sw))
    val rendered = sw.toString

    // No query-derived content in any field the log event exposes. ("Did you mean one of the
    // following?" is authored catalog template text, not query content, so it may appear; the
    // offending column and the interpolated suggestion value must not.)
    Seq(bodyMessage, exceptionMessage, exceptionType, redacted.toString, rendered).foreach {
      field =>
        field should not include column
        field should not include "Project"
        field should not include "SubqueryAlias"
        field should not include "== SQL =="
    }

    // Positive contract: the surviving content is stable and non-customer.
    bodyMessage should include(ExceptionMessages.QueryAnalysisErrorPrefix)
    exceptionMessage should include(
      "UNRESOLVED_COLUMN"
    ) // errorClass + static template log message
    exceptionMessage should include("<objectName>") // static placeholder retained, no value
    exceptionType shouldBe analysis.getClass.getName
    body.get("errorCode") shouldBe ErrorCode.QueryAnalysisError
    // The rendered throwable also retains the original type for debugging.
    redacted.toString should include("ExtendedAnalysisException")
  }

  // --------------------------------------------------------------------------
  // Production JSON serializer coverage + table-driven real-exception canary
  // matrix, scoped to the OPERATOR LOG path.
  //
  // The customer / persisted message intentionally retains the actionable
  // diagnostic (which can include the offending identifier), so the invariant
  // proven here is the strict one: the operator log message and the exact
  // string log4j receives never carry query-derived content. The tests drive
  // CustomLogging.convertToJson applied to the map constructLogEventMap builds
  // for the redacted throwable, then parse that line and recursively scan every
  // leaf value, so a canary that survived only after JSON escaping (a CRLF or
  // Unicode canary) is still caught.
  // --------------------------------------------------------------------------

  private val jsonMapper = new ObjectMapper()

  /**
   * The exact line log4j is handed: constructLogEventMap -> convertToJson, both reached by
   * reflection. convertToJson is `private static` and is the only place the OTEL log map becomes
   * the emitted string, so asserting on its output covers the serializer itself rather than just
   * the map it serializes.
   */
  private def productionLogLine(content: Object, throwable: Throwable): String = {
    val map = constructLogEventMap(content, throwable)
    val method =
      classOf[CustomLogging].getDeclaredMethod("convertToJson", classOf[java.util.Map[_, _]])
    method.setAccessible(true)
    method.invoke(null, map).asInstanceOf[String]
  }

  /** Every scalar leaf value in a JSON document, recursively. */
  private def leafValues(node: JsonNode): Seq[String] = {
    if (node.isObject) {
      node.fields().asScala.toSeq.flatMap(e => leafValues(e.getValue))
    } else if (node.isArray) {
      node.elements().asScala.toSeq.flatMap(leafValues)
    } else {
      Seq(node.asText())
    }
  }

  private def assertNoCanary(scope: String, text: String, canaries: Seq[String]): Unit =
    canaries.foreach { canary =>
      withClue(s"[$scope] leaked canary [$canary] in: $text\n") {
        text should not include canary
      }
    }

  /** Substring scan of the whole line plus a structural scan of every leaf. */
  private def assertJsonHasNoCanary(scope: String, json: String, canaries: Seq[String]): Unit = {
    assertNoCanary(s"$scope raw", json, canaries)
    val leaves = leafValues(jsonMapper.readTree(json))
    canaries.foreach { canary =>
      withClue(s"[$scope leaf] leaked canary [$canary] in $leaves\n") {
        leaves.exists(_.contains(canary)) shouldBe false
      }
    }
  }

  // The operator-log message for a Spark-native failure leads with the bracketed catalog
  // errorClass token and is a single line. What follows the token is the static catalog message
  // template (placeholders left un-interpolated) and, optionally, a safe cause class name. The
  // template is authored, non-customer text, so its exact content is not constrained here; the
  // no-canary guarantee is asserted separately by assertNoCanary. Matching `.*` (which excludes
  // newlines by default) also proves the message never spans multiple log lines.
  private val operatorLogShape =
    "^\\[[A-Z0-9_.]+\\]( .*)?$".r

  test(
    "production JSON serializer emits no query content in the operator log for an analysis failure") {
    spark.sql("SELECT 1 AS id, 'x' AS name").createOrReplaceTempView("real_serializer_probe")
    val column = "nonexistent_col_serializer_9099"
    val root = failWith(s"SELECT $column FROM real_serializer_probe")
    val redacted = FlintREPL.redactThrowable(root)

    val logLine = productionLogLine(operationMessage(redacted.getMessage, root), redacted)

    // The emitted line parses as JSON and no leaf value carries query content.
    assertJsonHasNoCanary(
      "serializer",
      logLine,
      Seq(column, "Project", "SubqueryAlias", "== SQL =="))

    // Positive contract: the OTEL envelope and stable tokens survive.
    val tree = jsonMapper.readTree(logLine)
    tree.at("/attributes/exception.type").asText() shouldBe root.getClass.getName
    tree.at("/body/errorCode").asText() shouldBe ErrorCode.QueryAnalysisError
    leafValues(tree).exists(_.contains("UNRESOLVED_COLUMN")) shouldBe true
  }

  private case class CanaryCase(
      name: String,
      sql: String,
      canaries: Seq[String],
      ansi: Boolean = false,
      sparkThrowable: Boolean = true,
      expectCanaryInRaw: Boolean = true,
      setup: () => Unit = () => ())

  private val longCanary = ("L" * 1500) + "_canary_long_9015"

  // Built from code points so the source stays ASCII (scalastyle forbids
  // non-ASCII), while the running query still carries real multibyte content.
  private val unicodeCanary =
    "canary_" + Seq(0x00fc, 0x00ef, 0x00f6, 0x00e9).map(_.toChar).mkString + "_9014"

  private def matrixCases: Seq[CanaryCase] = Seq(
    CanaryCase(
      "unresolved column",
      "SELECT canary_col_unrsv_9001 FROM real_matrix_vals",
      Seq("canary_col_unrsv_9001")),
    CanaryCase(
      "missing table or view",
      "SELECT * FROM canary_tbl_missing_9002",
      Seq("canary_tbl_missing_9002")),
    CanaryCase(
      "ambiguous reference",
      "SELECT canary_ambig_9003 FROM amb_a JOIN amb_b",
      Seq("canary_ambig_9003"),
      setup = () => {
        spark.sql("SELECT 1 AS canary_ambig_9003").createOrReplaceTempView("amb_a")
        spark.sql("SELECT 2 AS canary_ambig_9003").createOrReplaceTempView("amb_b")
      }),
    CanaryCase(
      "unresolved routine",
      "SELECT canary_fn_9004(id) FROM real_matrix_vals",
      Seq("canary_fn_9004")),
    CanaryCase(
      "datatype mismatch",
      "SELECT sum(canary_dtm_9005) FROM (SELECT array(1, 2) AS canary_dtm_9005)",
      Seq("canary_dtm_9005")),
    CanaryCase(
      "parse error",
      "SELCT canary_parse_9006 FROM real_matrix_vals",
      Seq("canary_parse_9006")),
    CanaryCase(
      "divide by zero (ANSI)",
      "SELECT canary_div_col_9007 / 0 FROM (SELECT 5 AS canary_div_col_9007)",
      Seq("canary_div_col_9007"),
      ansi = true),
    CanaryCase(
      "invalid cast (ANSI)",
      "SELECT CAST('canary_cast_9008' AS INT)",
      Seq("canary_cast_9008"),
      ansi = true),
    CanaryCase(
      "array index out of bounds (ANSI)",
      "SELECT array(canary_arr_9009)[10] FROM (SELECT 7 AS canary_arr_9009)",
      Seq("canary_arr_9009"),
      ansi = true),
    CanaryCase(
      "invalid regex pattern (executor exception, unknown-policy path)",
      "SELECT canary_rgx_9010col RLIKE '[' FROM (SELECT 'x' AS canary_rgx_9010col)",
      Seq("canary_rgx_9010col"),
      sparkThrowable = false,
      expectCanaryInRaw = false),
    CanaryCase(
      "nested CTE + subquery + join plan",
      "WITH cte AS (SELECT id, name FROM real_matrix_vals) " +
        "SELECT c.id FROM cte c JOIN real_matrix_vals r ON c.id = r.id " +
        "WHERE c.canary_nested_9011 > 0",
      Seq("canary_nested_9011")),
    CanaryCase(
      "ARN-shaped backticked identifier",
      "SELECT `arn:aws:iam::123456789012:role/canary_arn_9012` FROM real_matrix_vals",
      Seq("arn:aws:iam::123456789012:role/canary_arn_9012", "canary_arn_9012")),
    CanaryCase(
      "CRLF literal (ANSI)",
      "SELECT CAST('canary_crlf_9013\r\nSECOND_LINE_9013' AS INT)",
      Seq("canary_crlf_9013", "SECOND_LINE_9013"),
      ansi = true),
    CanaryCase(
      "Unicode backticked identifier",
      s"SELECT `$unicodeCanary` FROM real_matrix_vals",
      Seq(unicodeCanary)),
    CanaryCase(
      "very long backticked identifier",
      s"SELECT `$longCanary` FROM real_matrix_vals",
      Seq("_canary_long_9015")))

  matrixCases.foreach { c =>
    test(s"canary matrix (operator log): ${c.name}") {
      spark.sql("SELECT 1 AS id, 'x' AS name").createOrReplaceTempView("real_matrix_vals")
      c.setup()

      val root =
        if (c.ansi) {
          spark.conf.set("spark.sql.ansi.enabled", "true")
          try failWith(c.sql)
          finally spark.conf.unset("spark.sql.ansi.enabled")
        } else {
          failWith(c.sql)
        }

      // Precondition: the raw failure really carries the canary, so redaction is proven rather than
      // trivially satisfied. The executor-regex case is a negative control -- its query text never
      // reaches the pattern exception -- so it opts out of this precondition.
      if (c.expectCanaryInRaw) {
        val raw = Option(root.getMessage).getOrElse("")
        withClue(s"raw message did not contain any canary: $raw\n") {
          c.canaries.exists(raw.contains) shouldBe true
        }
      }

      // (1) the strict operator-log message carries no canary ...
      val log = ErrorSanitizer.operatorLogMessage(root)
      assertNoCanary("operatorLogMessage", log, c.canaries)

      // ... and for a Spark-native failure it leads with the bracketed errorClass token, followed
      // by the static catalog template (and an optional safe cause class name), on a single line.
      if (c.sparkThrowable) {
        withClue(s"operator log message was not a well-formed errorClass log line: $log\n") {
          operatorLogShape.pattern.matcher(log).matches() shouldBe true
        }
      }

      // (2) the exact production log line for the redacted throwable, scanned recursively across
      // body.message, exception.message/type, and attributes.
      val redacted = FlintREPL.redactThrowable(root)
      val logLine = productionLogLine(operationMessage(redacted.getMessage, root), redacted)
      assertJsonHasNoCanary("productionLogLine", logLine, c.canaries)
    }
  }
}
