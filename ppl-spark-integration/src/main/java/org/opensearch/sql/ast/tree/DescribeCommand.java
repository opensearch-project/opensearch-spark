/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import java.util.Collections;

/**
 * Extend Projection to describe the command itself
 */
public class DescribeCommand extends Project{
    private String commandDescription;

    public DescribeCommand(String commandDescription) {
        super(Collections.emptyList());
        this.commandDescription = commandDescription;
    }

    public String getDescription() {
        return commandDescription;
    }
}
