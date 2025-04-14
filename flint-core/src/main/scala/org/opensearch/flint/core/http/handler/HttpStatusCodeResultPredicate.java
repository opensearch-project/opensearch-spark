/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.http.handler;

import dev.failsafe.function.CheckedPredicate;
import java.util.Arrays;
import java.util.Set;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.apache.http.HttpResponse;

/**
 * Failure handler based on status code in HTTP response.
 *
 * @param <T> result type (supposed to be HttpResponse for OS client)
 */
public class HttpStatusCodeResultPredicate<T> implements CheckedPredicate<T> {

  private static final Logger LOG = Logger.getLogger(HttpStatusCodeResultPredicate.class.getName());

  /**
   * Retryable HTTP status code list
   */
  private final Set<Integer> retryableStatusCodes;

  public HttpStatusCodeResultPredicate(Set<Integer> httpStatusCodes) {
    this.retryableStatusCodes = httpStatusCodes;
  }

  @Override
  public boolean test(T result) throws Throwable {
    int statusCode = ((HttpResponse) result).getStatusLine().getStatusCode();
    boolean isRetryable = retryableStatusCodes.contains(statusCode);
    LOG.info("HTTP " + statusCode + (isRetryable ? " is " : " is not ") + "retryable");
    return isRetryable;
  }
}
