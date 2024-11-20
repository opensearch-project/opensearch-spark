/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.expression;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public enum DataSourceType {
  JSON,
  CSV, 
  PARQUET,
  TEXT
}
