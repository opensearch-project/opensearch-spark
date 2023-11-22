/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.http.handler;

import dev.failsafe.function.CheckedPredicate;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.http.HttpResponse;

/**
 * Failure handler based on status code in HTTP response.
 *
 * @param <T> result type (supposed to be HttpResponse for OS client)
 */
public class HttpStatusCodeHandler<T> implements CheckedPredicate<T> {

  /**
   * Retryable HTTP status code list
   */
  private final String[] httpStatusCodes;

  public HttpStatusCodeHandler(String httpStatusCodes) {
    this.httpStatusCodes = httpStatusCodes.split(",");
  }

  @Override
  public boolean test(T result) throws Throwable {
    Set<Integer> retryableStatusCodes =
        Arrays.stream(httpStatusCodes)
            .map(String::trim)
            .map(Integer::valueOf)
            .collect(Collectors.toSet());

    return retryableStatusCodes.contains(
        ((HttpResponse) result).getStatusLine().getStatusCode());
  }
}
