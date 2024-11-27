/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.http.handler;

import dev.failsafe.function.CheckedPredicate;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.entity.BufferedHttpEntity;
import org.apache.http.util.EntityUtils;

import java.util.Arrays;
import java.util.Set;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Failure handler based on content in HTTP response.
 *
 * @param <T> result type (supposed to be HttpResponse for OS client)
 */
public class HttpResponseMessageResultPredicate<T> implements CheckedPredicate<T> {

  private static final Logger LOG = Logger.getLogger(HttpResponseMessageResultPredicate.class.getName());

  /**
   * Retryable HTTP response message list
   */
  private final Set<String> retryableResponseMessages;

  public HttpResponseMessageResultPredicate(String messages) {
    this.retryableResponseMessages =
        Arrays.stream(messages.split(","))
            .map(String::trim)
            .collect(Collectors.toSet());
  }

  @Override
  public boolean test(T result) throws Throwable {
    HttpResponse response = (HttpResponse) result;
    HttpEntity entity = response.getEntity();
    if (entity != null) {
      // Buffer the entity so it can be read multiple times
      BufferedHttpEntity bufferedEntity = new BufferedHttpEntity(entity);
      response.setEntity(bufferedEntity);

      try {
        String responseContent = EntityUtils.toString(entity);
        LOG.info("Checking if response message is retryable");

        boolean isRetryable = retryableResponseMessages.stream()
            .anyMatch(responseContent::contains);

        LOG.info("Check retryable result: " + isRetryable);

        // Reset the entity's content
        bufferedEntity.getContent().reset();

        return isRetryable;
      } catch (Exception e) {
        LOG.info("Unable to parse response body. Retryable result: false");
        return false;
      }
    }
    LOG.info("No response entity found. Retryable result: false");
    return false;
  }
}
