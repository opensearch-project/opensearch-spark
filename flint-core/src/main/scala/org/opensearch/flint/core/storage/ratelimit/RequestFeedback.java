/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.storage.ratelimit;

public class RequestFeedback {
  public final boolean isTimeout;
  public final boolean hasRetryableFailure;
  public final long latency;
  public final int requestSize;

  public RequestFeedback(boolean isTimeout, boolean hasRetryableFailure, long latency, int requestSize) {
    this.isTimeout = isTimeout;
    this.hasRetryableFailure = hasRetryableFailure;
    this.latency = latency;
    this.requestSize = requestSize;
  }

  public static RequestFeedback noRetryable(long latency, int requestSize) {
    return new RequestFeedback(false, false, latency, requestSize);
  }

  public static RequestFeedback hasRetryable(long latency) {
    return new RequestFeedback(false, true, latency, 0);
  }

  public static RequestFeedback timeout() {
    return new RequestFeedback(true, false, 0, 0);
  }
}
