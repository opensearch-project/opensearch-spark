/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.storage;

import static org.opensearch.flint.core.metadata.log.FlintMetadataLogEntry.IndexState$;
import static org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_PRIMARY_TERM;
import static org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;

import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.logging.Logger;
import org.opensearch.flint.core.metadata.log.FlintMetadataLog;
import org.opensearch.flint.core.metadata.log.FlintMetadataLogEntry;
import org.opensearch.flint.core.metadata.log.OptimisticTransaction;

/**
 * Default optimistic transaction implementation that captures the basic workflow for
 * transaction support by optimistic locking.
 *
 * @param <T> result type
 */
public class OpenSearchOptimisticTransaction<T> implements OptimisticTransaction<T> {

  private static final Logger LOG = Logger.getLogger(OpenSearchOptimisticTransaction.class.getName());

  /**
   * Flint metadata log
   */
  private final FlintMetadataLog<FlintMetadataLogEntry> metadataLog;

  private Predicate<FlintMetadataLogEntry> initialCondition = null;
  private Function<FlintMetadataLogEntry, FlintMetadataLogEntry> transientAction = null;
  private Function<FlintMetadataLogEntry, FlintMetadataLogEntry> finalAction = null;

  public OpenSearchOptimisticTransaction(
      FlintMetadataLog<FlintMetadataLogEntry> metadataLog) {
    this.metadataLog = metadataLog;
  }

  @Override
  public OpenSearchOptimisticTransaction<T> initialLog(
      Predicate<FlintMetadataLogEntry> initialCondition) {
    this.initialCondition = initialCondition;
    return this;
  }

  @Override
  public OpenSearchOptimisticTransaction<T> transientLog(
      Function<FlintMetadataLogEntry, FlintMetadataLogEntry> action) {
    this.transientAction = action;
    return this;
  }

  @Override
  public OpenSearchOptimisticTransaction<T> finalLog(
      Function<FlintMetadataLogEntry, FlintMetadataLogEntry> action) {
    this.finalAction = action;
    return this;
  }

  @Override
  public T commit(Function<FlintMetadataLogEntry, T> operation) {
    Objects.requireNonNull(initialCondition);
    Objects.requireNonNull(transientAction);
    Objects.requireNonNull(finalAction);

    FlintMetadataLogEntry latest =
        metadataLog.getLatest().orElseGet(() -> metadataLog.add(emptyLogEntry()));

    if (initialCondition.test(latest)) {
      // TODO: log entry can be same?
      latest = metadataLog.add(transientAction.apply(latest));

      T result = operation.apply(latest);

      metadataLog.add(finalAction.apply(latest));
      return result;
    } else {
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
        "mys3", // TODO: get it from spark conf
        "");
  }
}
