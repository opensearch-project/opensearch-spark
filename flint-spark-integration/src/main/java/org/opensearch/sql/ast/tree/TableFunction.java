/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import com.google.common.collect.ImmutableList;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ast.expression.UnresolvedExpression;

import java.util.List;

/**
 * AST Node for Table Function.
 */


public class TableFunction extends UnresolvedPlan {

    private UnresolvedExpression functionName;

    private List<UnresolvedExpression> arguments;

    public TableFunction(UnresolvedExpression functionName, List<UnresolvedExpression> arguments) {
        this.functionName = functionName;
        this.arguments = arguments;
    }

    public List<UnresolvedExpression> getArguments() {
        return arguments;
    }

    public QualifiedName getFunctionName() {
        return (QualifiedName) functionName;
    }

    @Override
    public List<UnresolvedPlan> getChild() {
        return ImmutableList.of();
    }

    @Override
    public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
        return nodeVisitor.visitTableFunction(this, context);
    }

    @Override
    public UnresolvedPlan attach(UnresolvedPlan child) {
        return null;
    }
}
