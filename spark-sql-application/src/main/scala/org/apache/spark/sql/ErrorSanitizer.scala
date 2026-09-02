/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql

import scala.util.control.NonFatal

import com.amazonaws.AmazonServiceException
import com.amazonaws.services.glue.model.{AccessDeniedException => GlueAccessDeniedException, AWSGlueException}
import com.amazonaws.services.s3.model.AmazonS3Exception
import org.opensearch.flint.core.storage.OpenSearchBulkWriteException

import org.apache.spark.SparkThrowable
import org.apache.spark.SparkThrowableHelper
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.exception.RedactedException

/**
 * Shapes error information independently for persisted customer records, operator logs, and
 * machine-readable classification. Customer records keep one actionable, plan-free line. Operator
 * logs keep only typed fields, static Spark templates, and bounded cause categories.
 * Classification comes from typed fields and remains stable when either message changes. See each
 * public method for its per-family policy.
 */
object ErrorSanitizer extends Logging {

  /**
   * Message used when the sanitizer itself fails. Deliberately carries no detail from the
   * original throwable beyond the fact that something was redacted: if policy evaluation threw,
   * we cannot assert anything about what the original message contains, so returning it would be
   * fail-open.
   */
  private[sql] val RedactedFallbackMessage = "error details were redacted"

  /** Label emitted for a Spark-native failure that reports no catalog error class. */
  private[sql] val GenericSparkErrorLabel = "SPARK_ERROR"

  /**
   * Label emitted for a failure whose cause chain carries no recognized type -- no structured
   * bulk-write or S3 failure and no [[SparkThrowable]]. The shipped floor logged the deepest
   * cause's first message line, but for a query failure that line is customer-derived (it can
   * carry the offending identifier, a literal, or an OpenSearch document value), so it must not
   * reach the broadly-readable driver log. This label pairs with the deepest cause's bounded,
   * safe class name instead, giving an operator a type handle with no message text.
   */
  private[sql] val UnknownErrorLabel = "UNKNOWN_ERROR"

  /** Upper bound on a cause class name emitted into the operator log, as defense in depth. */
  private val MaxCauseClassNameLength = 256

  /**
   * Upper bound on the static catalog message template emitted into the operator log. The bundled
   * templates are short, so this is defense in depth against a pathological future catalog entry
   * rather than an expected truncation.
   */
  private val MaxTemplateLength = 1024

  /** Fixed platform classes recoverable from a generic Spark stage-failure message. */
  private val SafeMessageCauseClassNames = Set(
    "java.lang.OutOfMemoryError",
    "java.lang.StackOverflowError",
    "java.util.regex.PatternSyntaxException",
    "org.apache.spark.shuffle.FetchFailedException")

  /**
   * Captures the first exception class in Spark's generated `Lost task ... executor ...:` frame.
   */
  private val StageFailureCausePattern =
    ("(?s)\\bLost task\\b.*?\\):\\s*" +
      "([A-Za-z_$][A-Za-z0-9_$]*(?:\\.[A-Za-z_$][A-Za-z0-9_$]*)+" +
      "\\.[A-Za-z_$][A-Za-z0-9_$]*(?:Exception|Error))(?=:)").r

  /**
   * Closed vocabulary of error codes. A fixed enumeration is what makes this channel trustworthy:
   * nothing is interpolated into it, so it provably carries no customer content, and adding or
   * changing a value is an explicit, reviewable API change rather than an incidental side effect
   * of editing a message string.
   */
  object ErrorCode {
    val QuerySyntaxError = "QUERY_SYNTAX_ERROR"
    val QueryAnalysisError = "QUERY_ANALYSIS_ERROR"
    val SparkQueryError = "SPARK_QUERY_ERROR"
    val S3Error = "S3_ERROR"
    val GlueError = "GLUE_ERROR"
    val GlueAccessDenied = "GLUE_ACCESS_DENIED"
    val OpenSearchWriteError = "OPENSEARCH_WRITE_ERROR"
    val OpenSearchWriteAccessDenied = "OPENSEARCH_WRITE_ACCESS_DENIED"
    val Unknown = "UNKNOWN_ERROR"
  }

  /**
   * Machine-readable classification of a failure.
   *
   * @param errorCode
   *   a value from [[ErrorCode]]
   * @param statusCode
   *   HTTP status when the failure carries one on a typed field, otherwise None. Never parsed out
   *   of message text: a status recovered by regex from a string we also rewrite would
   *   reintroduce the coupling this class exists to remove.
   */
  case class ErrorClassification(errorCode: String, statusCode: Option[Int])

  /** Service-generated correlation identifiers safe to include in operator logs. */
  private[sql] case class OperatorLogContext(
      requestId: Option[String],
      extendedRequestId: Option[String])

  private[sql] def operatorLogContext(t: Throwable): OperatorLogContext = try {
    val chain = causeChain(t)
    val requestId =
      chain.collectFirst { case ase: AmazonServiceException => ase }.flatMap { ase =>
        safeNonEmpty(ase.getRequestId)
      }
    val extendedRequestId = chain.collectFirst { case s3: AmazonS3Exception => s3 }.flatMap {
      s3 => safeNonEmpty(s3.getExtendedRequestId)
    }
    OperatorLogContext(requestId, extendedRequestId)
  } catch {
    case NonFatal(_) => OperatorLogContext(None, None)
  }

  private def safeNonEmpty(value: => String): Option[String] =
    try Option(value).filter(_.nonEmpty)
    catch { case NonFatal(_) => None }

  /**
   * Classifies a throwable using typed fields only. Callers are expected to have unwrapped to the
   * root cause already; this additionally walks the cause chain so a wrapped typed failure is
   * still recognized.
   *
   * Specific failures win over generic ones regardless of nesting depth. A write or connector
   * failure surfaces to the driver wrapped in a task-failure [[SparkException]], so matching the
   * outermost throwable first would classify every such failure as a generic Spark error and lose
   * the actionable status.
   */
  def classify(t: Throwable): ErrorClassification = try {
    val chain = causeChain(t)
    chain
      .flatMap(classifySpecific)
      .headOption
      .orElse(chain.flatMap(classifyGeneric).headOption)
      .getOrElse(ErrorClassification(ErrorCode.Unknown, None))
  } catch {
    case NonFatal(e) =>
      // Log class names only. The subject throwable's message may carry customer content, and the
      // sanitizer's own failure `e` can wrap it, so neither is passed to the logger.
      logError(
        s"Failed to classify throwable of type [${t.getClass.getName}] " +
          s"(sanitizer error type [${e.getClass.getName}]); defaulting to unknown")
      ErrorClassification(ErrorCode.Unknown, None)
  }

  /**
   * Failures that carry an actionable, attributable status on a typed field. These are the cases
   * where classification must not be lost to an enclosing wrapper.
   */
  private def classifySpecific(t: Throwable): Option[ErrorClassification] = t match {
    case be: OpenSearchBulkWriteException =>
      val code =
        if (isOpenSearchAuthorizationFailure(be)) ErrorCode.OpenSearchWriteAccessDenied
        else ErrorCode.OpenSearchWriteError
      Some(ErrorClassification(code, Some(be.getStatusCode)))
    case s3: AmazonS3Exception =>
      Some(ErrorClassification(ErrorCode.S3Error, Some(s3.getStatusCode)))
    case glueDenied: GlueAccessDeniedException =>
      // Glue signals authorization failures with the concrete AccessDeniedException type but an SDK
      // HTTP status of 400, so the status alone would misclassify it. Match the type and preserve
      // whatever status the SDK reported rather than asserting 403.
      Some(ErrorClassification(ErrorCode.GlueAccessDenied, Some(glueDenied.getStatusCode)))
    case glue: AWSGlueException =>
      val code =
        if (glue.getStatusCode == 403) ErrorCode.GlueAccessDenied else ErrorCode.GlueError
      Some(ErrorClassification(code, Some(glue.getStatusCode)))
    case ase: AmazonServiceException =>
      Some(ErrorClassification(ErrorCode.Unknown, Some(ase.getStatusCode)))
    case _ => None
  }

  private def isOpenSearchAuthorizationFailure(be: OpenSearchBulkWriteException): Boolean =
    be.getStatusCode == 403 && be.getExceptionTypeNames.contains("security_exception")

  /** Query-lifecycle families, used only when nothing more specific exists in the chain. */
  private def classifyGeneric(t: Throwable): Option[ErrorClassification] = t match {
    case _: ParseException =>
      Some(ErrorClassification(ErrorCode.QuerySyntaxError, None))
    case _: AnalysisException =>
      Some(ErrorClassification(ErrorCode.QueryAnalysisError, None))
    // Any remaining Spark-native failure (SparkException and the SparkThrowable runtime-error
    // family, e.g. SparkArithmeticException) is a query error. Kept after the two more specific
    // cases above, which are themselves SparkThrowables.
    case _: SparkThrowable =>
      Some(ErrorClassification(ErrorCode.SparkQueryError, None))
    case _ => None
  }

  /**
   * Returns the persisted/forwarded diagnostic. Analysis and parse failures use the first line of
   * `getSimpleMessage`; other Spark failures use the first rendered line. OpenSearch bulk and S3
   * failures use typed structured fields. Other families retain the existing first-line floor for
   * compatibility. No path appends SQL state, plans, SQL blocks, or following multiline context.
   */
  def customerMessage(t: Throwable): String = try {
    customerPolicyMessage(t)
  } catch {
    case NonFatal(e) =>
      // Fail closed, and log class names only. Policy evaluation failing means we cannot assert
      // what the original message contains, so it is neither returned nor logged.
      logError(
        s"Failed to sanitize throwable of type [${t.getClass.getName}] " +
          s"(sanitizer error type [${e.getClass.getName}]); returning redacted message")
      RedactedFallbackMessage
  }

  /**
   * Returns the strict driver-log message. Typed OpenSearch/AWS fields win first. Spark failures
   * use the catalog `errorClass`, un-interpolated template, and at most one bounded cause class.
   * A generic no-cause Spark failure may recover only a fixed diagnostic class from message text.
   * Unrecognized failures emit [[UnknownErrorLabel]] plus the deepest bounded class name.
   */
  def operatorLogMessage(t: Throwable): String = try {
    operatorPolicyMessage(t)
  } catch {
    case NonFatal(e) =>
      logError(
        s"Failed to build operator log message for throwable of type [${t.getClass.getName}] " +
          s"(sanitizer error type [${e.getClass.getName}]); returning redacted message")
      RedactedFallbackMessage
  }

  /**
   * Wraps a throwable so that only its [[operatorLogMessage]] is ever exposed via `getMessage`,
   * `getLocalizedMessage`, `toString`, and the rendered stack-trace header, while preserving the
   * original exception type name and frames for debugging. This is the single redaction point for
   * the driver logs: a redacted throwable can be handed to any logger (directly, or via
   * [[org.apache.spark.sql.util.ThrowableHandler]]) without the original message text, query
   * context, or logical plan leaking through the logged exception. The persisted / forwarded
   * error string is built separately from [[customerMessage]].
   */
  def redactThrowable(t: Throwable): Throwable = {
    val redacted = new RedactedException(t.getClass.getName, operatorLogMessage(t))
    redacted.setStackTrace(t.getStackTrace)
    redacted
  }

  private def customerPolicyMessage(t: Throwable): String = t match {
    case be: OpenSearchBulkWriteException => openSearchBulkWriteMessage(be)
    case s3: AmazonS3Exception => s3StructuredMessage(s3)
    // AnalysisException and its subtypes must be matched before the generic SparkThrowable case
    // below, since they are themselves SparkThrowables but have a plan-free message accessor.
    case ae: AnalysisException => firstLine(ae.getSimpleMessage)
    case _: SparkThrowable => firstLine(t.getMessage)
    case other => firstLine(other.getMessage)
  }

  /**
   * Walks the cause chain so a wrapper cannot hide the failure it carries or downgrade it to the
   * raw first-line floor, mirroring [[classify]]. See [[operatorLogMessage]] for the resolution
   * order. When no recognized type is found, the floor emits a bare label plus the deepest
   * cause's safe class name via [[unrecognizedThrowableLogLabel]] rather than reading any message
   * text, so a customer-derived first line can never reach the driver log.
   */
  private def operatorPolicyMessage(t: Throwable): String = {
    val chain = causeChain(t)
    chain
      .collectFirst {
        case be: OpenSearchBulkWriteException => openSearchBulkWriteMessage(be)
        case s3: AmazonS3Exception => s3StructuredMessage(s3)
        case ase: AmazonServiceException => amazonServiceStructuredMessage(ase)
      }
      .orElse {
        if (chain.exists(_.isInstanceOf[SparkThrowable])) Some(sparkThrowableLogLabel(t))
        else None
      }
      .getOrElse(unrecognizedThrowableLogLabel(chain))
  }

  /**
   * The operator-log label for a cause chain that carries no recognized type. Emits only the bare
   * [[UnknownErrorLabel]] plus a bounded, safe class name for the deepest cause -- a code
   * identifier, never a customer value, consistent with [[safeCauseClassName]]. Unlike the
   * shipped floor it reads no message text, so an offending identifier, a literal, or an
   * OpenSearch document value that a query failure's message can carry never reaches the log.
   * `getClass` is read under a guard so even a pathological throwable yields the bare label
   * rather than a secondary failure.
   */
  private def unrecognizedThrowableLogLabel(chain: Stream[Throwable]): String = {
    val deepest = chain.last
    val className =
      try Some(boundName(deepest.getClass.getName))
      catch { case NonFatal(_) => None }
    className match {
      case Some(name) => s"[$UnknownErrorLabel] type=[$name]"
      case None => s"[$UnknownErrorLabel]"
    }
  }

  private def s3StructuredMessage(s3: AmazonS3Exception): String =
    s"serviceName=[${s3.getServiceName}], statusCode=[${s3.getStatusCode}], " +
      s"errorCode=[${s3.getErrorCode}]"

  private def amazonServiceStructuredMessage(ase: AmazonServiceException): String = {
    val fields = Seq(
      safeNonEmpty(ase.getServiceName).map(value => s"serviceName=[$value]"),
      Some(s"statusCode=[${ase.getStatusCode}]"),
      safeNonEmpty(ase.getErrorCode).map(value => s"errorCode=[$value]"))
    fields.flatten.mkString(", ")
  }

  /**
   * Returns the safe structured bulk-write message plus a temporary, canonical compatibility
   * token for existing downstream authorization translations. The token is derived only from
   * typed status and exception-name fields; no OpenSearch failure reason is copied. Legacy
   * consumers may require the exact `type=security_exception, reason=OpenSearch exception
   * [type=authorization_exception` shape in addition to
   * `exception.type=java.lang.RuntimeException`. Remove this token together with the
   * persisted-type shim once consumers classify on `errorCode` and `statusCode`.
   */
  private def openSearchBulkWriteMessage(be: OpenSearchBulkWriteException): String = {
    val message = firstLine(be.getMessage)
    val isAuthorizationFailure = isOpenSearchAuthorizationFailure(be)
    if (isAuthorizationFailure) {
      s"$message, type=security_exception, " +
        "reason=OpenSearch exception [type=authorization_exception]"
    } else {
      message
    }
  }

  /**
   * The strict operator-log label for a Spark-native failure.
   *
   *   - When the chain carries a catalog `errorClass`, that class is the leading token, followed
   *     by the static message *template* for the class when one resolves. The template is read
   *     from Spark's error-conditions catalog via [[catalogMessageTemplate]] with its `<param>`
   *     placeholders left un-interpolated, so it is provably free of parameter values, the
   *     offending identifier, the suggestion, any literal, and the query. It is the maximum
   *     diagnostic detail that is not customer-derived. When the class has no resolvable template
   *     (an unknown or malformed class) only the bracketed class is emitted, matching the
   *     previously shipped behavior.
   *   - For a generic Spark failure that reports no `errorClass`, the bare
   *     [[GenericSparkErrorLabel]] is emitted.
   *
   * In both cases a single bounded, safe cause class name is appended as `cause=[...]` when one
   * is available. It comes first from the structured immediate cause ([[safeCauseClassName]]);
   * only for the generic no-`errorClass` path, and only when there is no structured cause, is a
   * strictly-allowlisted platform class token recovered from the message text
   * ([[platformClassTokenFromMessage]]). No other message text is read. A cause class name is a
   * code identifier, never a customer value, so it is safe to log; for an opaque `INTERNAL_ERROR`
   * (whose template is only the `<message>` placeholder) it is often the one useful handle.
   */
  private def sparkThrowableLogLabel(t: Throwable): String =
    firstErrorClass(t) match {
      case Some(errorClass) =>
        val label = new StringBuilder("[").append(errorClass).append("]")
        catalogMessageTemplate(errorClass).foreach(template => label.append(" ").append(template))
        safeCauseClassName(t).foreach(cause => label.append(" cause=[").append(cause).append("]"))
        label.toString
      case None =>
        safeCauseClassName(t).orElse(platformClassTokenFromMessage(t)) match {
          case Some(causeName) => s"[$GenericSparkErrorLabel] cause=[$causeName]"
          case None => s"[$GenericSparkErrorLabel]"
        }
    }

  /**
   * The static catalog message template for an error class, normalized for a single log line, or
   * None when no safe template can be produced.
   *
   * The template is read from Spark's bundled error-conditions catalog
   * ([[org.apache.spark.SparkThrowableHelper.errorReader]]), which keeps the authored `<param>`
   * placeholders literal and does not substitute the throwable's parameter values. It is
   * therefore derived entirely from the platform's static resources and is independent of the
   * specific failure: no customer identifier, suggestion, literal, operand, or query text can
   * appear in it. `getMessageTemplate` throws for an unknown or malformed class name (a custom
   * throwable can return an arbitrary `errorClass`), so the lookup is guarded and yields None in
   * that case rather than emitting anything unvalidated. Newlines and repeated whitespace are
   * collapsed to single spaces so the template cannot introduce extra log lines, and the result
   * is length-bounded as defense in depth.
   */
  private def catalogMessageTemplate(errorClass: String): Option[String] =
    try {
      Option(SparkThrowableHelper.errorReader.getMessageTemplate(errorClass))
        .map(template => boundTemplate(template.replaceAll("\\s+", " ").trim))
        .filter(_.nonEmpty)
    } catch {
      case NonFatal(_) => None
    }

  private def boundTemplate(template: String): String =
    if (template.length <= MaxTemplateLength) template
    else template.substring(0, MaxTemplateLength)

  /**
   * First non-empty catalog error class in the cause chain, reading the typed field defensively.
   */
  private def firstErrorClass(t: Throwable): Option[String] =
    causeChain(t).flatMap {
      case st: SparkThrowable =>
        try Option(st.getErrorClass).filter(_.nonEmpty)
        catch { case NonFatal(_) => None }
      case _ => None
    }.headOption

  /**
   * The immediate cause's class name, bounded and guarded. A class name carries no customer
   * content, so it is safe to log, but the accessors are still guarded: a hostile `getCause` that
   * throws, or a self-referencing cause, yields None rather than a secondary failure or an
   * unbounded token.
   */
  private def safeCauseClassName(t: Throwable): Option[String] = {
    val cause =
      try t.getCause
      catch { case NonFatal(_) => null }
    if (cause == null || cause.eq(t)) {
      None
    } else {
      try Some(boundName(cause.getClass.getName))
      catch { case NonFatal(_) => None }
    }
  }

  private def boundName(name: String): String =
    if (name.length <= MaxCauseClassNameLength) name
    else name.substring(0, MaxCauseClassNameLength)

  /**
   * Recovers a fixed diagnostic class from Spark's generated DAGScheduler task-failure frame when
   * no structured cause exists. Only the first framed exception class is considered, so customer
   * message text cannot supply a later match.
   */
  private def platformClassTokenFromMessage(t: Throwable): Option[String] = {
    val message =
      try Option(t.getMessage).getOrElse("")
      catch { case NonFatal(_) => "" }
    StageFailureCausePattern
      .findFirstMatchIn(message)
      .map(_.group(1))
      .filter(SafeMessageCauseClassNames.contains)
      .map(boundName)
  }

  private def firstLine(message: String): String =
    Option(message).getOrElse("").split("\n", 2)(0)

  private def causeChain(t: Throwable): Stream[Throwable] = {
    def loop(current: Throwable, seen: Set[Throwable]): Stream[Throwable] = {
      if (current == null || seen.contains(current)) {
        Stream.empty
      } else {
        // getCause runs on the error-handling path; a hostile throwable whose getCause() throws
        // must not propagate a secondary failure. Treat a throwing getCause() as "no further cause".
        val next =
          try current.getCause
          catch { case NonFatal(_) => null }
        current #:: loop(next, seen + current)
      }
    }
    loop(t, Set.empty)
  }
}
