/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl.legacy.common.antlr;

public class SyntaxCheckException extends RuntimeException {
  public SyntaxCheckException(String message) {
    super(message);
  }
}
