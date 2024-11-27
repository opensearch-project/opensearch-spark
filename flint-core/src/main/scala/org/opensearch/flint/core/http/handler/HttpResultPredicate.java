/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.http.handler;

import dev.failsafe.function.CheckedPredicate;

/**
 * Failure handler based on HTTP response.
 *
 * @param <T> result type (supposed to be HttpResponse for OS client)
 */
public class HttpResultPredicate<T> implements CheckedPredicate<T> {

  private final HttpStatusCodeResultPredicate<T> statusCodePredicate;
  private final HttpResponseMessageResultPredicate<T> responseMessagePredicate;

  public HttpResultPredicate(HttpStatusCodeResultPredicate<T> statusCodePredicate, HttpResponseMessageResultPredicate<T> responseMessagePredicate) {
    this.statusCodePredicate = statusCodePredicate;
    this.responseMessagePredicate = responseMessagePredicate;
  }

  @Override
  public boolean test(T result) throws Throwable {
    return statusCodePredicate.test(result) || responseMessagePredicate.test(result);
  }
}
