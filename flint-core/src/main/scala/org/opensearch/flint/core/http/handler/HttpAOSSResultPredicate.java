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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Failure handler based on HTTP response from AOSS.
 *
 * @param <T> result type (supposed to be HttpResponse for OS client)
 */
public class HttpAOSSResultPredicate<T> implements CheckedPredicate<T> {

  private static final Logger LOG = LoggerFactory.getLogger(HttpAOSSResultPredicate.class);

  public static final int BAD_REQUEST_STATUS_CODE = 400;
  public static final String RESOURCE_ALREADY_EXISTS_EXCEPTION_MESSAGE = "resource_already_exists_exception";

  public HttpAOSSResultPredicate() { }

  @Override
  public boolean test(T result) throws Throwable {
    LOG.info("Checking if response is retryable");

    int statusCode = ((HttpResponse) result).getStatusLine().getStatusCode();
    if (statusCode != BAD_REQUEST_STATUS_CODE) {
      LOG.info("Status code " + statusCode + " is not " + BAD_REQUEST_STATUS_CODE + ". Check result: false");
      return false;
    }

    HttpResponse response = (HttpResponse) result;
    HttpEntity entity = response.getEntity();
    if (entity == null) {
      LOG.info("No response entity found. Check result: false");
      return false;
    }

    // Buffer the entity to make it repeatable, so that this retry test does not consume the content stream,
    // resulting in the request caller getting empty response
    BufferedHttpEntity bufferedEntity = new BufferedHttpEntity(entity);
    response.setEntity(bufferedEntity);

    try {
      String responseContent = EntityUtils.toString(bufferedEntity);
      // Effectively restores the content stream of the response
      bufferedEntity.getContent().reset();

      boolean isRetryable = responseContent.contains(RESOURCE_ALREADY_EXISTS_EXCEPTION_MESSAGE);

      LOG.info("Check retryable response result: " + isRetryable);
      return isRetryable;
    } catch (Exception e) {
      LOG.info("Unable to parse response body. Check result: false");
      return false;
    }
  }
}
