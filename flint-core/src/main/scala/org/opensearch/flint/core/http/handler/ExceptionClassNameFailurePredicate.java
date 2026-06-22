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
 * Failure handler that retries an exception when its class name appears in the configured list,
 * matched over the whole cause-chain (see {@link ErrorStacktraceFailurePredicate}).
 *
 * <p>Matching is by class name string, against both the exception's fully-qualified name
 * ({@link Class#getName()}) and its simple name ({@link Class#getSimpleName()}). A configured
 * entry therefore matches if it equals either form, so both fully-qualified names
 * (e.g. {@code java.net.ConnectException}) and simple names (e.g. {@code ConnectException}) are
 * accepted.
 *
 * <p>Simple-name matching is what makes the default connection-fault list shading agnostic: the
 * OpenSearch REST client relocates (shades) Apache HTTP, so at runtime the thrown class is e.g. a
 * relocated {@code <shaded-prefix>.org.apache.http.ConnectionClosedException}. Its
 * {@code getName()} carries the unknown shade prefix, but its {@code getSimpleName()} is still
 * {@code ConnectionClosedException}. Matching by name string (rather than loading the class and
 * calling {@code isInstance}) avoids both the unknown-prefix problem and loading classes.
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
    Class<? extends Throwable> clazz = throwable.getClass();
    // Match by fully-qualified name (backward compatible) or simple name (shading agnostic).
    return retryableExceptionClassNames.contains(clazz.getName())
        || retryableExceptionClassNames.contains(clazz.getSimpleName());
  }
}
