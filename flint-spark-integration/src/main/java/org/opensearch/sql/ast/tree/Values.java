/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import com.google.common.collect.ImmutableList;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.Node;
import org.opensearch.sql.ast.expression.Literal;

import java.util.List;

/**
 * AST node class for a sequence of literal values.
 */


public class Values extends UnresolvedPlan {

    private List<List<Literal>> values;

    public <T> Values(List<T> list) {
        
    }


    @Override
    public UnresolvedPlan attach(UnresolvedPlan child) {
        throw new UnsupportedOperationException("Values node is supposed to have no child node");
    }

    @Override
    public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
        return nodeVisitor.visitValues(this, context);
    }

    @Override
    public List<? extends Node> getChild() {
        return ImmutableList.of();
    }
}
