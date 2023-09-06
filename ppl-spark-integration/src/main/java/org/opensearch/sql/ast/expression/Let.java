/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.expression;

import com.google.common.collect.ImmutableList;
import org.opensearch.sql.ast.AbstractNodeVisitor;

import java.util.List;

/**
 * Represent the assign operation. e.g. velocity = distance/speed.
 */


public class Let extends UnresolvedExpression {
    private Field var;
    private UnresolvedExpression expression;

    public Let(Field var, UnresolvedExpression expression) {
        this.var = var;
        this.expression = expression;
    }

    public Field getVar() {
        return var;
    }

    public UnresolvedExpression getExpression() {
        return expression;
    }

    @Override
    public List<UnresolvedExpression> getChild() {
        return ImmutableList.of();
    }

    @Override
    public <R, C> R accept(AbstractNodeVisitor<R, C> nodeVisitor, C context) {
        return nodeVisitor.visitLet(this, context);
    }
}
