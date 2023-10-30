/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.metadata.log;

import java.util.Optional;

/**
 * Flint metadata log.
 */
public interface FlintMetadataLog<T> {

  T add(T logEntry);

  Optional<T> getLatest();
}
