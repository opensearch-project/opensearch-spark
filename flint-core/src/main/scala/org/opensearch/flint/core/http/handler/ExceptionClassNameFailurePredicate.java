/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.http.handler;

import static java.util.Collections.newSetFromMap;
import static java.util.logging.Level.SEVERE;

import dev.failsafe.function.CheckedPredicate;
import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.logging.Logger;

/**
 * Failure handler based on exception class type check.
 */
public class ExceptionClassNameFailurePredicate extends ErrorStacktraceFailurePredicate {

  private static final Logger LOG = Logger.getLogger(ErrorStacktraceFailurePredicate.class.getName());

  /**
   * Retryable exception class types.
   */
  private final Set<Class<? extends Throwable>> retryableExceptions;

  /**
   * @return exception class handler or empty handler (treat any exception non-retryable)
   */
  public static CheckedPredicate<? extends Throwable> create(Optional<String> exceptionClassNames) {
    if (exceptionClassNames.isEmpty()) {
      // This is required because Failsafe treats any Exception retryable by default
      return ex -> false;
    }
    return new ExceptionClassNameFailurePredicate(exceptionClassNames.get());
  }

  public ExceptionClassNameFailurePredicate(String exceptionClassNames) {
    // Use weak collection avoids blocking class unloading
    this.retryableExceptions = newSetFromMap(new WeakHashMap<>());
    Arrays.stream(exceptionClassNames.split(","))
        .map(String::trim)
        .map(this::loadClass)
        .forEach(retryableExceptions::add);
  }

  @Override
  protected boolean isRetryable(Throwable throwable) {
    for (Class<? extends Throwable> retryable : retryableExceptions) {
      if (retryable.isInstance(throwable)) {
        return true;
      }
    }
    return false;
  }

  private Class<? extends Throwable> loadClass(String className) {
    try {
      //noinspection unchecked
      return (Class<? extends Throwable>) Class.forName(className);
    } catch (ClassNotFoundException e) {
      String errorMsg = "Failed to load class " + className;
      LOG.log(SEVERE, errorMsg, e);
      throw new IllegalStateException(errorMsg);
    }
  }
}
