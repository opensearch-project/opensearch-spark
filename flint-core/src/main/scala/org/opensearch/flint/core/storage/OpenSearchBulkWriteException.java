/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.storage;

import org.opensearch.OpenSearchException;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.rest.RestStatus;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Thrown when an OpenSearch bulk write fails, carrying the failure classification as structured
 * fields rather than only as free text.
 *
 * <p>Previously this failure was raised as a bare {@code RuntimeException} wrapping
 * {@code BulkResponse.buildFailureMessage()}. That flattened every machine-readable attribute of
 * the response (per-item HTTP status, OpenSearch exception type) into one multi-line string, so the
 * only way for a downstream consumer to tell a customer authorization failure apart from a service
 * fault was to pattern-match the text. Any redaction or reformatting of that text then silently
 * changed the classification. Keeping {@link #getStatusCode()} and {@link #getExceptionTypeNames()}
 * as fields removes that coupling: the classification survives independently of how the message is
 * rendered or sanitized.
 *
 * <p>The message is built only from values that cannot contain customer document content: item
 * counts, HTTP statuses, and OpenSearch exception type names. Type names come from a closed
 * server-side vocabulary (for example {@code security_exception}) and never embed field values,
 * so they are safe to log and to forward downstream. The target index name remains available as a
 * structured field but is deliberately omitted from the message, because index names are
 * customer-controlled. The raw per-item failure text is also dropped, because item reasons can
 * echo document field values and generated document ids.
 */
public class OpenSearchBulkWriteException extends RuntimeException {

  /** Maximum depth walked when collecting exception type names from the failure cause chain. */
  private static final int MAX_CAUSE_DEPTH = 5;

  /**
   * OpenSearch 2.6 deserializes server-side exception types into the generic OpenSearchException
   * class and preserves the original type only in this structured message prefix. Capture only the
   * constrained type token; never retain or expose the following reason text.
   */
  private static final Pattern DESERIALIZED_EXCEPTION_TYPE =
      Pattern.compile("^OpenSearch exception \\[type=([a-z0-9_]+),\\s*reason=");

  private static final int FORBIDDEN_STATUS = RestStatus.FORBIDDEN.getStatus();

  private final String indexName;
  private final int statusCode;
  private final List<Integer> statusCodes;
  private final List<String> exceptionTypeNames;
  private final int failedItemCount;
  private final int totalItemCount;

  private OpenSearchBulkWriteException(
      String message,
      String indexName,
      int statusCode,
      List<Integer> statusCodes,
      List<String> exceptionTypeNames,
      int failedItemCount,
      int totalItemCount) {
    super(message);
    this.indexName = indexName;
    this.statusCode = statusCode;
    // Defensive immutable copies: callers must not be able to mutate the classification after the
    // fact, and the exception is frequently logged/forwarded from multiple threads.
    this.statusCodes = Collections.unmodifiableList(new ArrayList<>(statusCodes));
    this.exceptionTypeNames = Collections.unmodifiableList(new ArrayList<>(exceptionTypeNames));
    this.failedItemCount = failedItemCount;
    this.totalItemCount = totalItemCount;
  }

  /**
   * Builds an exception from a failed bulk response.
   *
   * <p>Every retained item failure contributes: {@link #getStatusCodes()} holds each distinct HTTP
   * status (ascending) and {@link #getExceptionTypeNames()} holds each distinct OpenSearch type
   * name (first-seen order). The single {@link #getStatusCode()} used for classification is derived
   * deterministically by {@link #representativeStatus(List)}: an authorization failure (403) wins
   * over any co-occurring status so a mixed batch that contains one is still classified as
   * access-denied; otherwise the first failure in item order is representative, which leaves a
   * single-failure response's classification unchanged.
   *
   * @param indexName the bulk target index
   * @param response the failed bulk response
   * @param isRetainedFailure predicate identifying item responses that count as real failures
   *     (create-conflicts are expected and excluded by the caller)
   * @return an exception carrying the structured failure classification
   */
  public static OpenSearchBulkWriteException from(
      String indexName, BulkResponse response, java.util.function.Predicate<BulkItemResponse> isRetainedFailure) {
    BulkItemResponse[] items = response.getItems();
    // isRetainedFailure excludes expected outcomes (create-conflicts), but on its own it also
    // admits successful non-CREATE items; require isFailed() so only real failures are counted.
    List<BulkItemResponse.Failure> failures =
        Arrays.stream(items)
            .filter(BulkItemResponse::isFailed)
            .filter(isRetainedFailure)
            .map(BulkItemResponse::getFailure)
            .filter(Objects::nonNull)
            .collect(Collectors.toList());

    List<Integer> orderedStatuses =
        failures.stream()
            .map(BulkItemResponse.Failure::getStatus)
            .map(RestStatus::getStatus)
            .collect(Collectors.toList());

    // Distinct statuses, ascending, so the exposed collection is deterministic regardless of the
    // order failures arrived in.
    List<Integer> distinctStatuses =
        orderedStatuses.stream().distinct().sorted().collect(Collectors.toList());

    // Type names across all failures, outermost-first within each and first-seen order across them,
    // de-duplicated. Values come from a closed server-side vocabulary and carry no document content.
    List<String> typeNames =
        failures.stream()
            .flatMap(f -> exceptionTypeNamesOf(f.getCause()).stream())
            .distinct()
            .collect(Collectors.toList());

    int statusCode = representativeStatus(orderedStatuses);

    StringBuilder message = new StringBuilder();
    message
        .append("OpenSearch bulk write failed: ")
        .append(failures.size())
        .append(" of ")
        .append(items.length)
        .append(" bulk items failed, statusCode=[")
        .append(statusCode)
        .append("]");
    if (distinctStatuses.size() > 1) {
      message
          .append(", statusCodes=[")
          .append(distinctStatuses.stream().map(String::valueOf).collect(Collectors.joining(", ")))
          .append("]");
    }
    if (!typeNames.isEmpty()) {
      // Rendered as "type=<name>" per entry, mirroring how OpenSearch itself names exception types,
      // so the classification stays readable in logs. Values come from a closed server-side
      // vocabulary and carry no document content.
      message
          .append(", ")
          .append(typeNames.stream().map(n -> "type=" + n).collect(Collectors.joining(", ")));
    }

    return new OpenSearchBulkWriteException(
        message.toString(), indexName, statusCode, distinctStatuses, typeNames,
        failures.size(), items.length);
  }

  /**
   * Deterministic representative status for classification.
   *
   * <p>Authorization failures are the actionable production case and must not be masked by a
   * co-occurring transient failure, so 403 wins whenever present. Otherwise the first failure in
   * item order is chosen, which is stable for a given response and preserves the prior
   * single-failure behavior. An empty set falls back to 500.
   */
  static int representativeStatus(List<Integer> orderedStatuses) {
    if (orderedStatuses.isEmpty()) {
      return RestStatus.INTERNAL_SERVER_ERROR.getStatus();
    }
    if (orderedStatuses.contains(FORBIDDEN_STATUS)) {
      return FORBIDDEN_STATUS;
    }
    return orderedStatuses.get(0);
  }

  /**
   * Collects the OpenSearch exception type names along a failure's cause chain, outermost first,
   * de-duplicated and depth-capped.
   *
   * <p>For locally constructed exceptions, {@code OpenSearchException.getExceptionName(Throwable)}
   * provides the server vocabulary. OpenSearch 2.6 wire deserialization instead reconstructs a
   * generic {@code OpenSearchException} and retains the original type only in the structured
   * {@code OpenSearch exception [type=..., reason=...]} prefix. In that case this method extracts
   * only the tightly constrained type token and discards the reason.
   */
  static List<String> exceptionTypeNamesOf(Throwable cause) {
    Set<String> names = new LinkedHashSet<>();
    Throwable current = cause;
    int depth = 0;
    while (current != null && depth < MAX_CAUSE_DEPTH) {
      names.add(exceptionTypeNameOf(current));
      if (current.getCause() == current) {
        break;
      }
      current = current.getCause();
      depth++;
    }
    return new ArrayList<>(names);
  }

  private static String exceptionTypeNameOf(Throwable throwable) {
    if (throwable instanceof OpenSearchException) {
      Matcher matcher = DESERIALIZED_EXCEPTION_TYPE.matcher(
          Objects.toString(throwable.getMessage(), ""));
      if (matcher.find()) {
        return matcher.group(1);
      }
    }
    return OpenSearchException.getExceptionName(throwable);
  }

  /** @return the bulk target index name */
  public String getIndexName() {
    return indexName;
  }

  /**
   * @return the representative HTTP status used for classification (403 if any failure was
   *     forbidden, otherwise the first failure's status), or 500 when no usable item failure existed
   */
  public int getStatusCode() {
    return statusCode;
  }

  /**
   * @return every distinct HTTP status across the retained item failures, ascending; immutable
   */
  public List<Integer> getStatusCodes() {
    return statusCodes;
  }

  /**
   * @return OpenSearch exception type names across all retained failures, de-duplicated in
   *     first-seen order; immutable
   */
  public List<String> getExceptionTypeNames() {
    return exceptionTypeNames;
  }

  /** @return number of item responses that counted as real failures */
  public int getFailedItemCount() {
    return failedItemCount;
  }

  /** @return total number of item responses in the bulk response */
  public int getTotalItemCount() {
    return totalItemCount;
  }
}
