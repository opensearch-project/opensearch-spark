/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.http.handler;

import static java.util.Collections.newSetFromMap;

import dev.failsafe.function.CheckedPredicate;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.WeakHashMap;

/**
 * Failure handler based on exception class type check.
 */
public class ExceptionClassNameHandler implements CheckedPredicate<Throwable> {

  /**
   * Retryable exception class types.
   */
  private final Set<Class<? extends Throwable>> retryableExceptions;

  /**
   * @return exception class handler or empty handler (treat any exception non-retryable)
   */
  public static CheckedPredicate<? extends Throwable> create(
      Optional<String> exceptionClassNames) {
    // By default, Failsafe handles any Exception
    if (exceptionClassNames.isEmpty()) {
      return ex -> false;
    }
    return new ExceptionClassNameHandler(exceptionClassNames.get());
  }

  public ExceptionClassNameHandler(String exceptionClassNames) {
    // Use weak collection avoids blocking class unloading
    this.retryableExceptions = newSetFromMap(new WeakHashMap<>());
    Arrays.stream(exceptionClassNames.split(","))
        .map(String::trim)
        .map(this::loadClass)
        .forEach(retryableExceptions::add);
  }

  @Override
  public boolean test(Throwable throwable) throws Throwable {
    // Consider retryable if exception found anywhere on stacktrace.
    // Meanwhile, handle nested exception to avoid dead loop by seen hash set.
    Set<Throwable> seen = new HashSet<>();
    while (throwable != null && seen.add(throwable)) {
      for (Class<? extends Throwable> retryable : retryableExceptions) {
        if (retryable.isInstance(throwable)) {
          return true;
        }
      }
      throwable = throwable.getCause();
    }
    return false;
  }

  private Class<? extends Throwable> loadClass(String className) {
    try {
      //noinspection unchecked
      return (Class<? extends Throwable>) Class.forName(className);
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException("Failed to load class " + className, e);
    }
  }
}
