/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.expression;

public enum ParseMethod {
  REGEX("regex"),
  GROK("grok"),
  PATTERNS("patterns");

   private final String name;

    ParseMethod(String name) {
        this.name = name;
    }
}
