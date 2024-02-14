/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.metadata.log;

import java.util.Optional;

/**
 * Flint metadata log that provides transactional support on write API based on different storage.
 */
public interface FlintMetadataLog<T> {

  /**
   * Add a new log entry to the metadata log.
   *
   * @param logEntry log entry
   * @return log entry after add
   */
  T add(T logEntry);

  /**
   * Get the latest log entry in the metadata log.
   *
   * @return latest log entry
   */
  Optional<T> getLatest();

  /**
   * Remove all log entries.
   */
  void purge();
}
