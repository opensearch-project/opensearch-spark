/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import com.google.common.collect.ImmutableList;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.UnresolvedExpression;

import java.util.List;

@Getter
@ToString
@EqualsAndHashCode(callSuper = false)
@RequiredArgsConstructor
public class Window extends UnresolvedPlan {
    private final List<UnresolvedExpression> windowFunctionList;
    private final List<UnresolvedExpression> partExprList;
    private final List<UnresolvedExpression> sortExprList;
    @Setter private UnresolvedExpression span;
    private UnresolvedPlan child;

    @Override
    public UnresolvedPlan attach(UnresolvedPlan child) {
        this.child = child;
        return this;
    }

    @Override
    public List<UnresolvedPlan> getChild() {
        return ImmutableList.of(this.child);
    }

    @Override
    public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
        return nodeVisitor.visitWindow(this, context);
    }
}
