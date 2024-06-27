/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.metadata.log;

import static java.util.logging.Level.SEVERE;
import static java.util.logging.Level.WARNING;
import static org.opensearch.flint.core.metadata.log.FlintMetadataLogEntry.IndexState$;
import static org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_PRIMARY_TERM;
import static org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;

import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.logging.Logger;

/**
 * Default optimistic transaction implementation that captures the basic workflow for
 * transaction support by optimistic locking.
 *
 * @param <T> result type
 */
public class DefaultOptimisticTransaction<T> implements OptimisticTransaction<T> {

  private static final Logger LOG = Logger.getLogger(DefaultOptimisticTransaction.class.getName());

  /**
   * Data source name. TODO: remove this in future.
   */
  private final String dataSourceName;

  /**
   * Flint metadata log
   */
  private final FlintMetadataLog<FlintMetadataLogEntry> metadataLog;

  private Predicate<FlintMetadataLogEntry> initialCondition = null;
  private Function<FlintMetadataLogEntry, FlintMetadataLogEntry> transientAction = null;
  private Function<FlintMetadataLogEntry, FlintMetadataLogEntry> finalAction = null;

  public DefaultOptimisticTransaction(
      String dataSourceName,
      FlintMetadataLog<FlintMetadataLogEntry> metadataLog) {
    this.dataSourceName = dataSourceName;
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
        metadataLog.getLatest().orElseGet(() -> metadataLog.add(emptyLogEntry()));

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

      // Copy latest seqNo and primaryTerm to initialLog for potential rollback use
      initialLog = initialLog.copy(
          initialLog.id(),
          latest.seqNo(),
          latest.primaryTerm(),
          initialLog.createTime(),
          initialLog.state(),
          initialLog.dataSource(),
          initialLog.error());
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

  private FlintMetadataLogEntry emptyLogEntry() {
    return new FlintMetadataLogEntry(
        "",
        UNASSIGNED_SEQ_NO,
        UNASSIGNED_PRIMARY_TERM,
        0L,
        IndexState$.MODULE$.EMPTY(),
        dataSourceName,
        "");
  }
}
