/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.metadata.log;

import static java.util.logging.Level.SEVERE;
import static java.util.logging.Level.WARNING;

import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.logging.Logger;

import org.opensearch.flint.common.metadata.log.FlintMetadataLog;
import org.opensearch.flint.common.metadata.log.FlintMetadataLogEntry;
import org.opensearch.flint.common.metadata.log.OptimisticTransaction;

/**
 * Default optimistic transaction implementation that captures the basic workflow for
 * transaction support by optimistic locking.
 *
 * @param <T> result type
 */
public class DefaultOptimisticTransaction<T> implements OptimisticTransaction<T> {

  private static final Logger LOG = Logger.getLogger(DefaultOptimisticTransaction.class.getName());

  /**
   * Flint metadata log
   */
  private final FlintMetadataLog<FlintMetadataLogEntry> metadataLog;

  private Predicate<FlintMetadataLogEntry> initialCondition = null;
  private Function<FlintMetadataLogEntry, FlintMetadataLogEntry> transientAction = null;
  private Function<FlintMetadataLogEntry, FlintMetadataLogEntry> finalAction = null;

  public DefaultOptimisticTransaction(FlintMetadataLog<FlintMetadataLogEntry> metadataLog) {
    this.metadataLog = metadataLog;
  }

  @Override
  public DefaultOptimisticTransaction<T> initialLog(
      Predicate<FlintMetadataLogEntry> initialCondition) {
    this.initialCondition = initialCondition;
    return this;
  }

  @Override
  public DefaultOptimisticTransaction<T> transientLog(
      Function<FlintMetadataLogEntry, FlintMetadataLogEntry> action) {
    this.transientAction = action;
    return this;
  }

  @Override
  public DefaultOptimisticTransaction<T> finalLog(
      Function<FlintMetadataLogEntry, FlintMetadataLogEntry> action) {
    this.finalAction = action;
    return this;
  }

  @Override
  public T commit(Function<FlintMetadataLogEntry, T> operation) {
    Objects.requireNonNull(initialCondition);
    Objects.requireNonNull(finalAction);

    // Get the latest log and create if not exists
    FlintMetadataLogEntry latest =
        metadataLog.getLatest().orElseGet(() -> metadataLog.add(metadataLog.emptyLogEntry()));

    // Perform initial log check
    if (!initialCondition.test(latest)) {
      LOG.warning("Initial log entry doesn't satisfy precondition " + latest);
      throw new IllegalStateException(
          String.format("Index state [%s] doesn't satisfy precondition", latest.state()));
    }

    // Append optional transient log
    FlintMetadataLogEntry initialLog = latest;
    if (transientAction != null) {
      latest = metadataLog.add(transientAction.apply(latest));

      // Copy latest entryVersion to initialLog for potential rollback use
      initialLog = initialLog.copy(
          initialLog.id(),
          initialLog.createTime(),
          initialLog.lastRefreshStartTime(), initialLog.lastRefreshCompleteTime(), initialLog.state(),
          latest.entryVersion(),
          initialLog.error(),
          initialLog.properties());
    }

    // Perform operation
    try {
      T result = operation.apply(latest);

      // Append final log or purge log entries
      FlintMetadataLogEntry finalLog = finalAction.apply(latest);
      if (finalLog == NO_LOG_ENTRY) {
        metadataLog.purge();
      } else {
        metadataLog.add(finalLog);
      }
      return result;
    } catch (Exception e) {
      LOG.log(SEVERE, "Rolling back transient log due to transaction operation failure", e);
      try {
        // Roll back transient log if any
        if (transientAction != null) {
          metadataLog.add(initialLog);
        }
      } catch (Exception ex) {
        LOG.log(WARNING, "Failed to rollback transient log", ex);
      }
      throw new IllegalStateException("Failed to commit transaction operation", e);
    }
  }
}
