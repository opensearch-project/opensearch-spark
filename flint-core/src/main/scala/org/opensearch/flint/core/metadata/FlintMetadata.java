/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.metadata;

/**
 * Flint metadata follows Flint index specification and defines metadata
 * for a Flint index regardless of query engine integration and storage.
 */
public class FlintMetadata {

  // TODO: define metadata format and create strong-typed class
  private final String content;

  // TODO: piggyback optional index settings and will refactor as above
  private String indexSettings;

  public FlintMetadata(String content) {
    this.content = content;
  }

  public FlintMetadata(String content, String indexSettings) {
    this.content = content;
    this.indexSettings = indexSettings;
  }

  public String getContent() {
    return content;
  }

  public String getIndexSettings() {
    return indexSettings;
  }

  public void setIndexSettings(String indexSettings) {
    this.indexSettings = indexSettings;
  }
}
