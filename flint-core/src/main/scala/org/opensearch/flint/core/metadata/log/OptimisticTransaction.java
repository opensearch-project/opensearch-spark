/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.metadata.log;

import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Optimistic transaction interface that represents a state transition on the state machine.
 * In particular, this abstraction is trying to express:
 * initial log (precondition)
 * => transient log (with pending operation to do)
 * => final log (after operation succeeds)
 * For example, "empty" => creating (operation is to create index) => active
 *
 * @param <T> result type
 */
public interface OptimisticTransaction<T> {

  /**
   * @param initialCondition initial precondition that the subsequent transition and action can proceed
   * @return this transaction
   */
  OptimisticTransaction<T> initialLog(Predicate<FlintMetadataLogEntry> initialCondition);

  /**
   * @param action action to generate transient log entry
   * @return this transaction
   */
  OptimisticTransaction<T> transientLog(Function<FlintMetadataLogEntry, FlintMetadataLogEntry> action);

  /**
   * @param action action to generate final log entry
   * @return this transaction
   */
  OptimisticTransaction<T> finalLog(Function<FlintMetadataLogEntry, FlintMetadataLogEntry> action);

  /**
   * Execute the given operation with the given log transition above.
   *
   * @param operation operation
   * @return result
   */
  T execute(Function<FlintMetadataLogEntry, T> operation);

  /**
   * No optimistic transaction.
   */
  class NoOptimisticTransaction<T> implements OptimisticTransaction<T> {
    @Override
    public OptimisticTransaction<T> initialLog(Predicate<FlintMetadataLogEntry> initialCondition) {
      return this;
    }

    @Override
    public OptimisticTransaction<T> transientLog(Function<FlintMetadataLogEntry, FlintMetadataLogEntry> action) {
      return this;
    }

    @Override
    public OptimisticTransaction<T> finalLog(Function<FlintMetadataLogEntry, FlintMetadataLogEntry> action) {
      return this;
    }

    @Override
    public T execute(Function<FlintMetadataLogEntry, T> operation) {
      return operation.apply(null);
    }
  };
}
