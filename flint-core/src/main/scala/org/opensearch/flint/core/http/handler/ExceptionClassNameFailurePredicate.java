/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.http.handler;

import dev.failsafe.function.CheckedPredicate;
import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Failure handler that retries an exception when a configured class name matches the exception or
 * any of its superclasses, over the whole cause-chain (see {@link ErrorStacktraceFailurePredicate}).
 *
 * <p>For each exception, its class hierarchy is walked (the class and every superclass) and a
 * configured entry matches if it equals that class's fully-qualified name
 * ({@link Class#getName()}) or its simple name ({@link Class#getSimpleName()}). So both
 * fully-qualified names (e.g. {@code java.net.ConnectException}) and simple names
 * (e.g. {@code ConnectException}) are accepted, and a configured name also matches subclasses
 * (e.g. {@code ConnectException} matches {@code org.apache.http.conn.HttpHostConnectException},
 * which is what connection-refused surfaces as).
 *
 * <p>Walking the hierarchy by name (rather than loading the configured class and calling
 * {@code isInstance}) keeps subtype matching while remaining shading agnostic: the OpenSearch REST
 * client relocates (shades) Apache HTTP, so at runtime the thrown class is e.g. a relocated
 * {@code <shaded-prefix>.org.apache.http.ConnectionClosedException}. Its {@code getName()} carries
 * the unknown shade prefix, but its {@code getSimpleName()} is still {@code ConnectionClosedException}.
 * Matching the simple name over the hierarchy avoids both the unknown-prefix problem and loading
 * classes from a configured (possibly unshaded or non-existent) name.
 */
public class ExceptionClassNameFailurePredicate extends ErrorStacktraceFailurePredicate {

  private static final Logger LOG = LoggerFactory.getLogger(ExceptionClassNameFailurePredicate.class);

  /**
   * Retryable exception class names (each entry is either a fully-qualified or a simple name).
   */
  private final Set<String> retryableExceptionClassNames;

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
    this.retryableExceptionClassNames = Arrays.stream(exceptionClassNames.split(","))
        .map(String::trim)
        .filter(name -> !name.isEmpty())
        .collect(Collectors.toSet());
    LOG.info("Retryable exception class names: " + retryableExceptionClassNames);
  }

  @Override
  protected boolean isRetryable(Throwable throwable) {
    // Walk the class hierarchy so a configured name also matches subclasses (e.g. the default
    // "ConnectException" matches HttpHostConnectException). Match by fully-qualified name
    // (backward compatible) or simple name (shading agnostic).
    for (Class<?> clazz = throwable.getClass();
        clazz != null && clazz != Object.class;
        clazz = clazz.getSuperclass()) {
      if (retryableExceptionClassNames.contains(clazz.getName())
          || retryableExceptionClassNames.contains(clazz.getSimpleName())) {
        return true;
      }
    }
    return false;
  }
}
