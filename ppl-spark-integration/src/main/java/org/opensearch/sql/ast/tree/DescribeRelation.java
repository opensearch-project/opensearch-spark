/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import org.opensearch.sql.ast.expression.UnresolvedExpression;

/**
 * Extend Relation to describe the table itself
 */
public class DescribeRelation extends Relation{
    public DescribeRelation(UnresolvedExpression tableName) {
        super(tableName);
    }
}
