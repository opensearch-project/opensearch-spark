/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.function.Predicate;
import org.junit.jupiter.api.Test;
import org.opensearch.OpenSearchSecurityException;
import org.opensearch.action.DocWriteRequest.OpType;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkItemResponse.Failure;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.rest.RestStatus;

/**
 * Verifies that a bulk write failure carries its classification as structured fields, so a consumer
 * never has to pattern-match the message text to tell an authorization failure from a service fault.
 */
class OpenSearchBulkWriteExceptionTest {

  private static final Predicate<BulkItemResponse> IS_FAILURE =
      itemResp -> itemResp.getOpType() != OpType.CREATE
          || (itemResp.getFailure() != null && itemResp.getFailure().getStatus() != RestStatus.CONFLICT);

  private static BulkResponse responseOf(BulkItemResponse... items) {
    return new BulkResponse(items, 100L);
  }

  @Test
  public void capturesForbiddenStatusAsAStructuredField() {
    // Reported production shape: authorization details exist only in the original multiline failure
    // text, while status is available as a typed field on the item failure.
    BulkItemResponse forbidden = new BulkItemResponse(0, OpType.INDEX,
        new Failure("myindex", "doc-1", new OpenSearchSecurityException("no permissions"),
            RestStatus.FORBIDDEN));

    OpenSearchBulkWriteException e =
        OpenSearchBulkWriteException.from("myindex", responseOf(forbidden), IS_FAILURE);

    // Classification survives independently of the message wording -- this is the property that the
    // first-newline truncation destroyed when the status lived only in the text.
    assertEquals(403, e.getStatusCode());
    assertEquals(1, e.getFailedItemCount());
    assertEquals(1, e.getTotalItemCount());
    assertEquals("myindex", e.getIndexName());
  }

  @Test
  public void collectsOpenSearchExceptionTypeNamesAlongTheCauseChain() {
    List<String> names = OpenSearchBulkWriteException.exceptionTypeNamesOf(
        new OpenSearchSecurityException("denied", new IllegalArgumentException("bad")));

    // Names come from OpenSearch's own type vocabulary, outermost first.
    assertEquals(List.of("security_exception", "illegal_argument_exception"), names);
  }

  @Test
  public void deduplicatesRepeatedTypeNamesAndToleratesSelfReferencingCause() {
    List<String> names = OpenSearchBulkWriteException.exceptionTypeNamesOf(
        new IllegalStateException("outer", new IllegalStateException("inner")));

    assertEquals(List.of("illegal_state_exception"), names);
  }

  @Test
  public void messageCarriesClassificationButNotPerItemFailureText() {
    String documentValue = "customer-secret-field-value";
    BulkItemResponse forbidden = new BulkItemResponse(0, OpType.INDEX,
        new Failure("myindex", "doc-1",
            new OpenSearchSecurityException("no permissions for " + documentValue),
            RestStatus.FORBIDDEN));
    BulkResponse response = responseOf(forbidden);

    OpenSearchBulkWriteException e =
        OpenSearchBulkWriteException.from("myindex", response, IS_FAILURE);

    // The classification a consumer needs is present ...
    assertTrue(e.getMessage().contains("statusCode=[403]"), e.getMessage());
    assertTrue(e.getMessage().contains("type=security_exception"), e.getMessage());
    // ... while customer-controlled content is not carried over: neither the per-item reason nor
    // the target index name appears in the rendered message. The index remains available through
    // getIndexName() for typed internal handling.
    assertFalse(e.getMessage().contains(documentValue), e.getMessage());
    assertFalse(e.getMessage().contains("myindex"), e.getMessage());
    // Precondition: the message we replaced really did contain that value, so this proves a strip.
    assertTrue(response.buildFailureMessage().contains(documentValue));
  }

  @Test
  public void excludesCreateConflictsFromTheFailureCount() {
    BulkItemResponse conflict = new BulkItemResponse(0, OpType.CREATE,
        new Failure("myindex", "doc-1", null, RestStatus.CONFLICT));
    BulkItemResponse forbidden = new BulkItemResponse(1, OpType.INDEX,
        new Failure("myindex", "doc-2", new OpenSearchSecurityException("denied"),
            RestStatus.FORBIDDEN));

    OpenSearchBulkWriteException e =
        OpenSearchBulkWriteException.from("myindex", responseOf(conflict, forbidden), IS_FAILURE);

    // Create-conflicts are an expected outcome, so they must not inflate the failure count nor
    // become the representative status.
    assertEquals(1, e.getFailedItemCount());
    assertEquals(2, e.getTotalItemCount());
    assertEquals(403, e.getStatusCode());
  }

  @Test
  public void doesNotCountSuccessfulItemsAsFailures() {
    // The caller's retained-failure predicate (!isCreateConflict) admits successful non-CREATE
    // items, because isCreateConflict only matches OpType.CREATE. The failure count must therefore
    // additionally require isFailed(), or a mixed bulk would overcount.
    BulkItemResponse succeededIndex = new BulkItemResponse(0, OpType.INDEX, docWriteResponse());
    BulkItemResponse forbidden = new BulkItemResponse(1, OpType.INDEX,
        new Failure("myindex", "doc-2", new OpenSearchSecurityException("denied"),
            RestStatus.FORBIDDEN));

    OpenSearchBulkWriteException e = OpenSearchBulkWriteException.from(
        "myindex", responseOf(succeededIndex, forbidden), IS_FAILURE);

    assertEquals(1, e.getFailedItemCount());
    assertEquals(2, e.getTotalItemCount());
    assertEquals(403, e.getStatusCode());
  }

  private static org.opensearch.action.DocWriteResponse docWriteResponse() {
    return new org.opensearch.action.index.IndexResponse(
        new org.opensearch.index.shard.ShardId("myindex", "uuid", 0), "doc-0", 1L, 1L, 1L, true);
  }

  @Test
  public void fallsBackToServerErrorWhenNoItemFailureIsUsable() {
    // Defensive: a response that reports failures but exposes no usable item failure must still
    // yield a definite classification rather than throwing.
    OpenSearchBulkWriteException e =
        OpenSearchBulkWriteException.from("myindex", responseOf(), IS_FAILURE);

    assertEquals(500, e.getStatusCode());
    assertEquals(0, e.getFailedItemCount());
  }

  @Test
  public void aggregatesEveryFailureStatusAndTypeNameAcrossAllItems() {
    BulkItemResponse forbidden = new BulkItemResponse(0, OpType.INDEX,
        new Failure("myindex", "doc-1", new OpenSearchSecurityException("denied"),
            RestStatus.FORBIDDEN));
    BulkItemResponse throttled = new BulkItemResponse(1, OpType.INDEX,
        new Failure("myindex", "doc-2", new IllegalStateException("busy"),
            RestStatus.TOO_MANY_REQUESTS));

    OpenSearchBulkWriteException e = OpenSearchBulkWriteException.from(
        "myindex", responseOf(forbidden, throttled), IS_FAILURE);

    // Every distinct status is retained (ascending), not just the first failure's.
    assertEquals(List.of(403, 429), e.getStatusCodes());
    // Type names from all failures, de-duplicated in first-seen order.
    assertEquals(List.of("security_exception", "illegal_state_exception"), e.getExceptionTypeNames());
    assertEquals(2, e.getFailedItemCount());
    assertEquals(2, e.getTotalItemCount());
  }

  @Test
  public void authorizationStatusWinsAsRepresentativeInAMixedFailureRegardlessOfOrder() {
    // The throttled item is first, so a "first failure wins" rule would return 429 and mask the
    // authorization failure. 403 must win deterministically.
    BulkItemResponse throttled = new BulkItemResponse(0, OpType.INDEX,
        new Failure("myindex", "doc-1", new IllegalStateException("busy"),
            RestStatus.TOO_MANY_REQUESTS));
    BulkItemResponse forbidden = new BulkItemResponse(1, OpType.INDEX,
        new Failure("myindex", "doc-2", new OpenSearchSecurityException("denied"),
            RestStatus.FORBIDDEN));

    OpenSearchBulkWriteException e = OpenSearchBulkWriteException.from(
        "myindex", responseOf(throttled, forbidden), IS_FAILURE);

    assertEquals(403, e.getStatusCode());
    assertEquals(List.of(403, 429), e.getStatusCodes());
  }

  @Test
  public void representativeStatusIsFirstInItemOrderWhenNoAuthorizationFailurePresent() {
    // No 403 present, so the representative is deterministic: the first failure in item order.
    BulkItemResponse throttled = new BulkItemResponse(0, OpType.INDEX,
        new Failure("myindex", "doc-1", new IllegalStateException("busy"),
            RestStatus.TOO_MANY_REQUESTS));
    BulkItemResponse serverError = new BulkItemResponse(1, OpType.INDEX,
        new Failure("myindex", "doc-2", new IllegalStateException("boom"),
            RestStatus.INTERNAL_SERVER_ERROR));

    OpenSearchBulkWriteException e = OpenSearchBulkWriteException.from(
        "myindex", responseOf(throttled, serverError), IS_FAILURE);

    assertEquals(429, e.getStatusCode());
    assertEquals(List.of(429, 500), e.getStatusCodes());
  }

  @Test
  public void exposedCollectionsAreImmutable() {
    BulkItemResponse forbidden = new BulkItemResponse(0, OpType.INDEX,
        new Failure("myindex", "doc-1", new OpenSearchSecurityException("denied"),
            RestStatus.FORBIDDEN));
    OpenSearchBulkWriteException e =
        OpenSearchBulkWriteException.from("myindex", responseOf(forbidden), IS_FAILURE);

    assertThrows(UnsupportedOperationException.class, () -> e.getStatusCodes().add(500));
    assertThrows(UnsupportedOperationException.class, () -> e.getExceptionTypeNames().add("x"));
  }
}
