/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.metadata.log;

import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

public interface OptimisticTransaction<T> {

  OptimisticTransaction<T> initialLog(Predicate<FlintMetadataLogEntry> initialCondition);

  OptimisticTransaction<T> transientLog(Function<FlintMetadataLogEntry, FlintMetadataLogEntry> action);

  OptimisticTransaction<T> finalLog(Function<FlintMetadataLogEntry, FlintMetadataLogEntry> action);

  T execute(Supplier<T> action);
}
