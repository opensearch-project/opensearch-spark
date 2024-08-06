/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.common.metadata.log;

import java.util.function.Function;
import java.util.function.Predicate;

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
   * Constant that indicate log entry should be purged.
   */
  FlintMetadataLogEntry NO_LOG_ENTRY = null;

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
   * @param action action to generate final log entry (will delete entire metadata log if NO_LOG_ENTRY)
   * @return this transaction
   */
  OptimisticTransaction<T> finalLog(Function<FlintMetadataLogEntry, FlintMetadataLogEntry> action);

  /**
   * Execute the given operation with the given log transition above.
   *
   * @param operation operation
   * @return result
   */
  T commit(Function<FlintMetadataLogEntry, T> operation);
}
