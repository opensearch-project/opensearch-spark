/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.sql.ast.tree;

public interface Help {
    /**
     * command description (help)
     * @return
     */
    String describe();

    /**
     *  command samples (help)
     * @return
     */
    String sample();
}
