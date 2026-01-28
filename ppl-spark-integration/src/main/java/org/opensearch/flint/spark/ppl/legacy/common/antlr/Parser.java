/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl.legacy.common.antlr;

import org.antlr.v4.runtime.tree.ParseTree;

public interface Parser {
  ParseTree parse(String query);
}
