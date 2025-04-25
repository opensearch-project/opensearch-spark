/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.storage.ratelimit;

public class RequestFeedback {
  public final boolean isTimeout;
  public final boolean hasRetryableFailure;
  public final long latency;

  public RequestFeedback(boolean isTimeout, boolean hasRetryableFailure, long latency) {
    this.isTimeout = isTimeout;
    this.hasRetryableFailure = hasRetryableFailure;
    this.latency = latency;
  }

  public static RequestFeedback noRetryable(long latency) {
    return new RequestFeedback(false, false, latency);
  }

  public static RequestFeedback hasRetryable(long latency) {
    return new RequestFeedback(false, true, latency);
  }

  public static RequestFeedback timeout() {
    return new RequestFeedback(true, false, 0);
  }
}
