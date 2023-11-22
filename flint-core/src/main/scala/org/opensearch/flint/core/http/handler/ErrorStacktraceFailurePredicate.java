/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.http.handler;

import dev.failsafe.function.CheckedPredicate;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Logger;

/**
 * Failure predicate that determines if retryable based on error stacktrace iteration.
 */
public abstract class ErrorStacktraceFailurePredicate implements CheckedPredicate<Throwable> {

  private static final Logger LOG = Logger.getLogger(ErrorStacktraceFailurePredicate.class.getName());

  /**
   * This base class implementation iterates the stacktrace and pass each exception
   * to subclass for retryable decision.
   */
  @Override
  public boolean test(Throwable throwable) throws Throwable {
    // Use extra set to Handle nested exception to avoid dead loop
    Set<Throwable> seen = new HashSet<>();

    while (throwable != null && seen.add(throwable)) {
      LOG.info("Checking if exception retryable: " + throwable);

      if (isRetryable(throwable)) {
        LOG.info("Exception is retryable: " + throwable);
        return true;
      }
      throwable = throwable.getCause();
    }

    LOG.info("No retryable exception found on the stacktrace");
    return false;
  }

  /**
   * Is exception retryable decided by subclass implementation
   */
  protected abstract boolean isRetryable(Throwable throwable);
}
