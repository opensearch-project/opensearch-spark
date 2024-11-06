/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import lombok.ToString;
import org.opensearch.sql.ast.expression.UnresolvedExpression;

import java.util.Collections;

/**
 * Extend Relation to describe the table itself
 */
@ToString
public class DescribeRelation extends Relation{
    public DescribeRelation(UnresolvedExpression tableName) {
        super(Collections.singletonList(tableName));
    }
}
