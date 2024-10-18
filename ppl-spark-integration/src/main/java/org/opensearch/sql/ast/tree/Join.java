/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.google.common.collect.ImmutableMap;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.UnresolvedExpression;

@ToString
@Getter
@RequiredArgsConstructor
@EqualsAndHashCode(callSuper = false)
public class Join extends UnresolvedPlan {
    private UnresolvedPlan left;
    private final UnresolvedPlan right;
    private final String leftAlias;
    private final String rightAlias;
    private final JoinType joinType;
    private final Optional<UnresolvedExpression> joinCondition;
    private final JoinHint joinHint;

    @Override
    public UnresolvedPlan attach(UnresolvedPlan child) {
        this.left = new SubqueryAlias(leftAlias, child);
        return this;
    }

    @Override
    public List<UnresolvedPlan> getChild() {
        return ImmutableList.of(left);
    }

    @Override
    public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
        return nodeVisitor.visitJoin(this, context);
    }

    public enum JoinType {
        INNER,
        LEFT,
        RIGHT,
        SEMI,
        ANTI,
        CROSS,
        FULL
    }

    @Getter
    @RequiredArgsConstructor
    public static class JoinHint {
        private final Map<String, String> hints;

        public JoinHint() {
            this.hints = ImmutableMap.of();
        }
    }
}