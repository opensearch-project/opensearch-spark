/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.metadata.log;

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
    if (initialCondition.test(latest)) {

      // Append optional transient log
      if (transientAction != null) {
        latest = metadataLog.add(transientAction.apply(latest));
      }

      // Perform operation
      T result = operation.apply(latest);

      // Append final log
      metadataLog.add(finalAction.apply(latest));
      return result;
    } else {
      LOG.warning("Initial log entry doesn't satisfy precondition " + latest);
      throw new IllegalStateException(
          "Transaction failed due to initial log precondition not satisfied");
    }
  }

  private FlintMetadataLogEntry emptyLogEntry() {
    return new FlintMetadataLogEntry(
        "",
        UNASSIGNED_SEQ_NO,
        UNASSIGNED_PRIMARY_TERM,
        IndexState$.MODULE$.EMPTY(),
        dataSourceName,
        "");
  }
}
