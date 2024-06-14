/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.metadata.log;

import org.opensearch.flint.core.FlintOptions;
import org.opensearch.flint.core.storage.FlintOpenSearchMetadataLogService;

/**
 * {@link FlintMetadataLogService} builder.
 */
public class FlintMetadataLogServiceBuilder {
  public static FlintMetadataLogService build(FlintOptions options) {
    return new FlintOpenSearchMetadataLogService(options);
  }
}
