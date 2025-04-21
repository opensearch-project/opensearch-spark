/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.storage.ratelimit;

public class RequestFeedback {
  public final boolean isSuccess;
  public final long latency;
  public final boolean isTimeout;

  public RequestFeedback(boolean isSuccess, long latency, boolean isTimeout) {
    this.isSuccess = isSuccess;
    this.latency = latency;
    this.isTimeout = isTimeout;
  }

  public static RequestFeedback success(long latency) {
    return new RequestFeedback(true, latency, false);
  }

  public static RequestFeedback failure(long latency) {
    return new RequestFeedback(false, latency, false);
  }

  public static RequestFeedback timeout() {
    return new RequestFeedback(false, 0, true);
  }
}
